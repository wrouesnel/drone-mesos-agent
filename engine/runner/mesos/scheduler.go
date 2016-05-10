/*
	Mesos scheduling parts of the mesosEngine
*/

package mesos_engine

import (
	"errors"
	"io"
	"sync"

	log "github.com/Sirupsen/logrus"

	"github.com/gogo/protobuf/proto"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/backoff"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/mesos-go/scheduler/calls"

	"fmt"
	"github.com/creack/httpreq"
	"github.com/mesos/mesos-go/httpcli/httpsched"
	"net/url"
	"time"
)

var (
	// name of the build agent container.
	DefaultAgent = "drone/drone-exec:latest"
	// default name of the build agent executable
	DefaultEntrypoint = []string{"/bin/drone-exec"}
	// default argument to invoke build steps
	DefaultBuildArgs = []string{"--pull", "--cache", "--clone", "--build", "--deploy"}
	// default argument to invoke build steps
	DefaultPullRequestArgs = []string{"--pull", "--cache", "--clone", "--build"}
	// default arguments to invoke notify steps
	DefaultNotifyArgs = []string{"--pull", "--notify"}
)

// Defines the strategy to adopt when scheduling tasks. "Pack" fits as many jobs
// as possible per offer. "Loose" fits 1 job per offer. Loose gives other
// frameworks more opportunity to allocate their own tasks.
// Pack is the default.
type schedulingStrategy int32

const (
	PackStrategy  schedulingStrategy = 0
	LooseStrategy schedulingStrategy = 1
)

// Holds Mesos specific framework configuration. This is parsed out of the config
// URL. Some are direct mappings to Mesos configuration, others are used indirectly
// This is the drone-side representation, it still has to be converted to Mesos
type mesosConfig struct {
	// Direct Mesos parameters
	user       string
	name       string
	role       string
	checkpoint bool
	principal  string
	//labels              Labels
	//reviveBurst         int	// EXECUTOR ONLY
	//reviveWait          time.Duration // EXECUTOR ONLY

	// Endpoint configuration
	schedulerScheme string
	schedulerHost   string
	connectTimeout  time.Duration

	// Drone-specific
	strategy schedulingStrategy
}

// Returns a Mesos config parsed from a query string
// TODO: we can write test cases for this!
func newMesosConfig(u *url.URL) (mesosConfig, error) {
	// Set defaults
	this := mesosConfig{
		user:       "root",
		name:       "drone CI",
		role:       "drone_ci",
		checkpoint: false,
		principal:  "",
		//reviveBurst: 5,
		//reviveWait: time.Second * 10,
		schedulerScheme: u.Scheme,
		schedulerHost:   u.Host,
		connectTimeout:  time.Second * 10,
	}

	err := httpreq.NewParsingMap().
		ToString("user", &this.user).
		ToString("name", &this.name).
		ToString("role", &this.role).
		ToBool("checkpoint", &this.checkpoint).
		ToString("principal", &this.principal).
		// TODO: labels
		//ToInt("reviveburst", &this.reviveBurst).
		// TODO: reviveWait
		Parse(u.Query())

	return this, err
}

// Implements the Mesos event handler interface for drone.
type mesosScheduler struct {
	// Current tasks we're tracking
	tasks    map[mesos.TaskID]chan<- mesos.TaskStatus
	tasksMtx sync.RWMutex

	// Pending tasks are read from this channel. As a result, assignTaskToNode
	// intentionally blocks until task assignment is complete.
	pendingTasks    map[mesos.TaskID]mesos.TaskInfo
	pendingTasksMtx sync.Mutex

	// Mesos state
	frameworkId       string  // Framework ID
	heartbeatInterval float64 // Expected heartbeat interval

	subscribe          *scheduler.Call
	registrationTokens <-chan struct{}
	cli                httpsched.Caller // mesos-go HTTP client
}

func newMesosScheduler(mesosCfg mesosConfig) *mesosScheduler {
	this := mesosScheduler{
		tasks:        make(map[mesos.TaskID]chan<- mesos.TaskStatus),
		pendingTasks: make(map[mesos.TaskID]mesos.TaskInfo),
		cli: httpsched.NewClient(
			httpcli.New(
				httpcli.Endpoint(fmt.Sprintf("%s://%s/api/v1/scheduler", mesosCfg.schedulerScheme, mesosCfg.schedulerHost)),
				httpcli.Codec(&encoding.ProtobufCodec),
				httpcli.Do(httpcli.With(httpcli.Timeout(mesosCfg.connectTimeout))),
			),
		),
	}

	fwInfo := &mesos.FrameworkInfo{
		User:       mesosCfg.user,
		Name:       mesosCfg.name,
		Role:       proto.String(mesosCfg.role),
		Checkpoint: proto.Bool(mesosCfg.checkpoint),
		// Hostname: TODO: we should set this from something for the drone web ui
		Principal: proto.String(mesosCfg.principal),
		Capabilities: []mesos.FrameworkInfo_Capability{
			mesos.FrameworkInfo_Capability{Type: mesos.REVOCABLE_RESOURCES},
		},
		// Labels TODO: support in the mesos struct
	}

	this.subscribe = calls.Subscribe(true, fwInfo)
	this.registrationTokens = backoff.Notifier(1*time.Second, 15*time.Second, nil)

	// Launch the framework in a go-routine.
	go func() {
		for {
			// Resubscribe handling
			if fwInfo.GetFailoverTimeout() > 0 && this.frameworkId != "" {
				this.subscribe.Subscribe.FrameworkInfo.ID =
					&mesos.FrameworkID{Value: this.frameworkId}
			}
			<-this.registrationTokens
			log.Infoln("Connecting to Mesos")
			resp, subscribedCaller, err := this.cli.Call(this.subscribe)

			if resp != nil {
				defer resp.Close()
			}

			// If we failed, we'll fallthrough to the end of the function
			if err == nil {
				this.frameworkId = "" // Forget old ID since we (re)subscribed
				if subscribedCaller != nil {
					oldCaller := this.cli
					this.cli = subscribedCaller
					defer func() {
						this.cli = oldCaller
					}()
				}
			}

			// Enter main event loop
			for err == nil {
				var e scheduler.Event
				if err = resp.Decoder().Invoke(&e); err == nil {
					err = this.HandleEvent(&e)
				}
			}

			if err != nil && err != io.EOF {
				log.Errorln("Error subscribing to Mesos:", err)
			} else if err != nil {
				log.Errorln("Disconnected from Mesos!")
			}
		}
	}()

	return &this
}

// Logging for our scheduler
func (this *mesosScheduler) log() *log.Entry {
	return log.WithFields(log.Fields{
		"frameworkId": this.frameworkId,
	})
}

// This implements the mesos event handler interface for us. It is simply a
// a dispatcher which calls the relevant function.
func (this *mesosScheduler) HandleEvent(e *scheduler.Event) error {
	if e.Type == nil {
		log.Errorln("Got Mesos event with type nil")
		return errors.New("HandleEvent: Nil event type received")
	}

	this.log().Debugln("Event Received:", e.GetType().String())

	// This is safe now because we sanity checked
	switch *e.Type {
	case scheduler.Event_SUBSCRIBED:
		this.frameworkId = e.Subscribed.GetFrameworkID().GetValue()
		this.heartbeatInterval = e.Subscribed.GetHeartbeatIntervalSeconds()
		return nil

	case scheduler.Event_OFFERS:
		return this.ResourceOffers(e.Offers.GetOffers())

	case scheduler.Event_RESCIND:
		return this.OfferRescinded(e.Rescind.GetOfferID())

	case scheduler.Event_UPDATE:
		return this.StatusUpdate(e.Update.GetStatus())

	case scheduler.Event_MESSAGE:
		return this.FrameworkMessage(e.Message.ExecutorID, e.Message.AgentID, e.Message.Data)

	case scheduler.Event_FAILURE:
		if e.Failure.AgentID != nil {
			return this.AgentLost(*e.Failure.AgentID, e.Failure.GetStatus())
		} else if e.Failure.ExecutorID != nil {
			// Executor failure (shouldn't happen, we don't use one)
			return this.ExecutorLost(*e.Failure.ExecutorID, *e.Failure.AgentID, e.Failure.GetStatus())
		}
		return nil

	// Sent by the master when an asynchronous error event is generated
	// (e.g., a framework is not authorized to subscribe with the given role).
	// It is recommended that the framework abort when it receives an error
	// and retry subscription as necessary.
	case scheduler.Event_ERROR:
		// TODO: we need to do a proper abort and retry
		return errors.New(e.Error.GetMessage())

	// Periodic message sent by the Mesos master according to
	// 'Subscribed.heartbeat_interval_seconds'. If the scheduler does
	// not receive any events (including heartbeats) for an extended
	// period of time (e.g., 5 x heartbeat_interval_seconds), there is
	// likely a network partition. In such a case the scheduler should
	// close the existing subscription connection and resubscribe
	// using a backoff strategy.
	case scheduler.Event_HEARTBEAT:
		return nil
	}

	// TODO: probably should throw an error
	return errors.New("Unknown, unhandled event.")
}

// Blocks until a task is assigned to a node, and returns the channel on which
// status updates can be read. Implements some basic Mesos sanity checking
// for scheduler state.
func (this *mesosScheduler) assignTaskToNode(t mesos.TaskInfo) (<-chan mesos.TaskStatus, error) {
	this.tasksMtx.RLock()
	if _, found := this.tasks[t.GetTaskID()]; found {
		this.tasksMtx.RUnlock()
		return nil, errors.New("mesosScheduler: attempted to reuse still in-use TaskID")
	}
	this.tasksMtx.RUnlock()

	// Create the status return channel
	statusCh := make(chan mesos.TaskStatus)

	// Add the task to our local map of tasks -> channel
	this.tasksMtx.Lock()
	defer this.tasksMtx.Unlock()
	this.tasks[t.GetTaskID()] = statusCh

	// Add the task to the pending tasks list for assignment
	this.pendingTasksMtx.Lock()
	defer this.pendingTasksMtx.Unlock()
	this.pendingTasks[t.TaskID] = t

	// Return the status channel to the job caller
	return statusCh, nil
}

// Process a resource offers event and launch tasks.
func (this *mesosScheduler) ResourceOffers(offers []mesos.Offer) error {
	for _, offer := range offers {
		log := this.log().WithField("offerId", offer.GetID().Value).
			WithField("agentId", offer.GetAgentID().Value)

		remainingResources := mesos.Resources(offer.Resources)
		assignableTasks := []mesos.TaskInfo{}

		log.Debugln("Received offer with resources:", remainingResources.String())

		// Lock and assign pending tasks
		this.pendingTasksMtx.Lock()
		for _, taskInfo := range this.pendingTasks {
			log := log.WithField("taskId", taskInfo.GetTaskID())

			flattenedResources := remainingResources.Flatten()

			// Check if we can fit in this task
			if flattenedResources.ContainsAll(mesos.Resources(taskInfo.Resources)) == false {
				log.Debugln("Not assigning task to offer due to insufficient remaining resources")
				continue
			}

			// TODO: it is possible that we might want to be hostname pinned.
			// but I can't find any specific evidence of this. If so, we might
			// change the task map to return a hostname, so drone can enforce
			// pinning.

			// Assign the task to the offer
			taskInfo.AgentID = offer.AgentID
			log.Infoln("Assigning task to offer:", taskInfo.GetName())
			assignableTasks = append(assignableTasks, taskInfo)
			remainingResources.Subtract(taskInfo.Resources...)
		}

		// Only log if we're actually dispatching tasks.
		if len(assignableTasks) > 0 {
			log.Infoln("Launching tasks assigned to offer:", len(assignableTasks))
		} else {
			log.Debugln("No tasks assigned to the offer.")
		}

		// Try and do the actual task launch.
		accept := calls.Accept(
			calls.OfferWithOperations(offer.ID, calls.OpLaunch(assignableTasks...)),
		)
		if err := this.cli.CallNoData(accept); err != nil {
			log.Errorln("Failed while trying to accept tasks:", err)
		} else {
			// Remove the tasks we've assigned from the pending tasks list once we
			// have successfully dispatched to Mesos.
			for _, t := range assignableTasks {
				delete(this.pendingTasks, t.TaskID)
			}
		}
		this.pendingTasksMtx.Unlock()
	}

	// TODO: should we return an error if task launch requests fail?
	return nil
}

func (this *mesosScheduler) StatusUpdate(status mesos.TaskStatus) error {
	log := log.WithField("taskId", status.GetTaskID())
	log.Infoln("Status Update:", status.GetState().String(), status.GetMessage())

	// Look up the task channel and notify it's listener
	this.tasksMtx.RLock()
	taskCh, found := this.tasks[status.TaskID]
	if !found {
		log.Errorln("Received status update, but TaskID is not known to us.")
		this.tasksMtx.RUnlock()
		return nil
	}

	// Notify listener
	taskCh <- status
	this.tasksMtx.RUnlock()

	// If the task state means the task stopped, then it should no longer be
	// tracked by the scheduler - the engine should figure out if it wants to
	// reschedule (and how). This also frees up the task ID for re-use.
	switch taskState := status.GetState(); taskState {
	case mesos.TASK_FINISHED, mesos.TASK_LOST, mesos.TASK_KILLED,
		mesos.TASK_FAILED, mesos.TASK_ERROR:

		this.tasksMtx.Lock()
		close(taskCh)
		delete(this.tasks, status.GetTaskID())
		this.tasksMtx.Unlock()

		log.Infoln("Removed terminated task.")
	}

	return nil
}

func (this *mesosScheduler) OfferRescinded(oid mesos.OfferID) error {
	// Normally not relevant, and executors will fail so it's okay. But we may
	// switch to using revocable offers in the near future.
	this.log().Errorln("offer rescinded: %v", oid)
	return nil
}

func (this *mesosScheduler) FrameworkMessage(executorId mesos.ExecutorID, agentId mesos.AgentID, data []byte) error {
	this.log().Errorln("Framework message: ", executorId, agentId, data)
	return nil
}

func (this *mesosScheduler) AgentLost(agentId mesos.AgentID, code int32) error {
	// TODO: notify all tasks that their build is dead and will not recover.
	// The result should be everything gets reset and rescheduled elsewhere.
	this.log().Errorln("Mesos agent lost:", agentId, code)
	return nil
}

func (this *mesosScheduler) ExecutorLost(executorId mesos.ExecutorID, agentId mesos.AgentID, code int32) error {
	this.log().Errorln("Executor lost:", executorId, agentId, code)
	return nil
}

// Allows killing a task
func (this *mesosScheduler) KillTask(taskId mesos.TaskID) error {
	killCall := calls.Kill(taskId.GetValue(), "")
	err := this.cli.CallNoData(killCall)
	if err != nil {
		this.log().Errorln("Error trying to kill task:", taskId, err)
	}
	return err
}
