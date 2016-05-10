/*
	Implements a Mesos build pool. Jobs are assigned to a Mesos cluster using
	the docker containerizer and with CPU and RAM limits. This backend gives a
	lot of stuff for free because Mesos/ZK will handle most of our normal pool
	management concerns.
*/

package mesos_engine

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"

	. "github.com/drone/drone/engine"
	"github.com/drone/drone/model"
	"github.com/drone/drone/shared/envconfig"
	"github.com/drone/drone/store"

	. "github.com/drone/drone/engine/util"
	"github.com/samalba/dockerclient"

	"github.com/drone/drone/engine/runner"
	"github.com/mesos/mesos-go"
)

// TODO: how do we handle composed spin ups?
// Represents resources reserved on a host for a build
type taskReservation struct {
	droneTask *Task // The task drone attached to this reservation

	slaveID string // The slave ID these resources were taken from

	//cpu float64		// CPU reserved for the task
	//mem float64		// Memory reservation
	//disk float64	// Disk space reservation
}

// Implements the drone Engine interface for a Mesos backend.
type mesosEngine struct {
	// Handlers for drone
	bus       *Eventbus
	Updater   *Updater
	scheduler *mesosScheduler

	// Environment variables which will be passed to invoked jobs by this engine
	envs envconfig.Env

	// Maps SlaveIDs to tasks. This is because drone is going to take over the
	// the docker.socket on the Slave to run subtasks, and we need locality when
	// we do high level assignments.
	reservations   map[string]*taskReservation
	reservationMtx sync.RWMutex

	// Maps Job IDs to mesos TaskIDs so we can kill them.
	jobMap    map[int64]string
	jobMapMtx sync.RWMutex
}

func NewEngine(env envconfig.Env, s store.Store) Engine {
	this := &mesosEngine{}
	this.bus = NewEventbus()
	this.Updater = &Updater{this.bus}
	// Copy the environment to the engine config so it'll be mirrored to containers
	this.envs = env.Copy()

	// Read ENGINE_CONFIG URI
	config, err := url.Parse(env.Get("ENGINE_CONFIG"))
	if err != nil {
		log.Errorln("Bad engine configuration: ", err)
		return nil
	}

	// Parse a Mesos framework configuration from it (or use defaults)
	mesosCfg, err := newMesosConfig(config)
	if err != nil {
		log.Errorln("Invaid Mesos configuration specified: ", err)
		return nil
	}

	this.scheduler = newMesosScheduler(mesosCfg)
	if this.scheduler == nil {
		log.Errorln("Mesos scheduler failed to initialize")
		return nil
	}

	return this
}

func (this *mesosEngine) ContainerStart(container *runner.Container) (string, error) {

}

func (this *mesosEngine) ContainerStop(name string) error {

}

func (this *mesosEngine) ContainerRemove(name string) error {

}

func (this *mesosEngine) ContainerWait(name string) (*runner.State, error) {

}

func (this *mesosEngine) ContainerLogs(name string) (io.ReadCloser, error) {

}

// Overview of Drone scheduling process for a task:
// -> Assign a node
// -> Assign node to every job
// -> Run each job in the task in order on the node
// -> Run notification jobs
// Mesos is completely asynchronous however, and moreover doesn't necessarily
// want to let us schedule things onto the same node. To work around this,
// we first reserve the resources the build will need, store the slaveID we
// reserved onto, and then schedule our jobs onto the node when we get
// re-offered.
func (this *mesosEngine) Schedule(c context.Context, req *Task) {
	// Reserve a CPU and some RAM on any node.
	// TODO: parse the job for resource allocation sizes

	// since we are probably running in a go-routine
	// make sure we recover from any panics so that
	// a bug doesn't crash the whole system.
	//defer func() {
	//	if err := recover(); err != nil {
	//
	//		const size = 64 << 10
	//		buf := make([]byte, size)
	//		buf = buf[:runtime.Stack(buf, false)]
	//		log.Errorf("panic running build: %v\n%s", err, string(buf))
	//	}
	//	//e.pool.release(node)
	//}()

	// TODO: best way to track node ID?
	// update the node that was allocated to each job
	//func(id int64) {
	//	for _, job := range req.Jobs {
	//		job.NodeID = id
	//		store.UpdateJob(c, job)
	//	}
	//}(node.ID)

	req.Build.Started = time.Now().UTC().Unix()
	req.Build.Status = model.StatusRunning
	this.Updater.SetBuild(c, req)

	// Initially blank - we wait for the first job to determine the slave we
	// should stick too.
	slaveId := ""

	for idx, job := range req.Jobs {
		req.Job = job // Set the current job being run in the task

		// TODO: we should have a plan for errors. Built-in doesn't though.
		allocatedSlaveId, _ := this.runJob(c, req, this.Updater, slaveId)

		// The first job is unique compared to the others, because it sets the slave
		// we want to assign all the rest of our jobs too. runJob returns the slaveId
		// for this reason.
		if idx == 0 {
			slaveId = allocatedSlaveId
			log.Infoln("Initial tasks executed on slave:", slaveId)
		} else {
			if slaveId != allocatedSlaveId {
				log.Errorf("Got a different slaveId for a job after we'd already been allocated one: had=%s got=%s",
					slaveId, allocatedSlaveId)
			}
		}
	}

	// update overall status based on each job
	req.Build.Status = model.StatusSuccess
	for _, job := range req.Jobs {
		if job.Status != model.StatusSuccess {
			log.Infof("Marking build unsuccessful due to job %d not being successful", job.ID)
			req.Build.Status = job.Status
			break
		}
	}
	req.Build.Finished = time.Now().UTC().Unix()
	if err := this.Updater.SetBuild(c, req); err != nil {
		log.Errorf("error updating build completion status. %s", err)
	}

	// run notifications
	if err := this.runJobNotify(req, slaveId); err != nil {
		log.Errorf("error executing notification step. %s", err)
	}
}

func (this *mesosEngine) Cancel(build int64, job int64, node *model.Node) error {
	// TODO: we need to centralize how we construct Mesos task IDs for this engine
	id := fmt.Sprintf("drone_build_%d_job_%d", build, job)
	return this.scheduler.KillTask(mesos.TaskID{Value: id})
}

func (this *mesosEngine) Stream(build int64, job int64, node *model.Node) (io.ReadCloser, error) {
	// TODO: we need some way to connect to Mesos job output dynamically
	return nil, nil
}

func (this *mesosEngine) Deallocate(node *model.Node) {
	// noop - deallocate has no meaning with the mesos backend
}

func (this *mesosEngine) Allocate(node *model.Node) error {
	// noop - allocate has no meaning with the mesos backend
	return nil
}

func (this *mesosEngine) Subscribe(c chan *Event) {
	this.bus.Subscribe(c)
}

func (this *mesosEngine) Unsubscribe(c chan *Event) {
	this.bus.Unsubscribe(c)
}

func (this *mesosEngine) runJob(c context.Context, r *Task, Updater *Updater, slaveId string) (string, error) {
	var err error
	// Setup structure logging for this function
	log := log.WithField("build", r.Build.ID).WithField("job", r.Job.ID)

	name := fmt.Sprintf("drone_build_%d_job_%d", r.Build.ID, r.Job.ID)

	defer func() {
		if r.Job.Status == model.StatusRunning {
			r.Job.Status = model.StatusError
			r.Job.Finished = time.Now().UTC().Unix()
			r.Job.ExitCode = 255
		}
		if r.Job.Status == model.StatusPending {
			r.Job.Status = model.StatusError
			r.Job.Started = time.Now().UTC().Unix()
			r.Job.Finished = time.Now().UTC().Unix()
			r.Job.ExitCode = 255
		}
		Updater.SetJob(c, r)

		this.scheduler.KillTask(mesos.TaskID{Value: name})
	}()

	// encode the build payload to write to stdin
	// when launching the build container
	in, err := EncodeToLegacyFormat(r)
	if err != nil {
		log.Errorf("failure to marshal work. %s", err)
		return "", err
	}

	// CREATE AND START BUILD
	args := DefaultBuildArgs
	if r.Build.Event == model.EventPull {
		args = DefaultPullRequestArgs
	}
	args = append(args, "--")
	args = append(args, string(in))

	conf := &dockerclient.ContainerConfig{
		Image:      DefaultAgent,
		Entrypoint: DefaultEntrypoint,
		Cmd:        args,
		Env:        this.envs.ToSlice(),
		HostConfig: dockerclient.HostConfig{
			Binds:            []string{"/var/run/docker.sock:/var/run/docker.sock"},
			MemorySwappiness: -1,
		},
		Volumes: map[string]struct{}{
			"/var/run/docker.sock": struct{}{},
		},
	}

	// Convert the docker command to a mesos docker task.
	mesosTask := dockerToMesosTask(name, conf)
	log.Debugln("converted docker job to mesos task")

	// Assign resource allocations
	// TODO: how should we support drone resource requests in Mesos?
	// 1: we probably need global limits
	// 2: we'd like to be able to do per-repo overrides
	resources := mesos.Resources{}
	resources.Add(
		*mesos.BuildResource().Name("cpu").Scalar(1).Resource,
		*mesos.BuildResource().Name("mem").Scalar(512).Resource,
	)
	mesosTask.Resources = resources

	// Assign the task to the cluster and get the update channels.
	respCh, err := this.scheduler.assignTaskToNode(mesosTask)
	if err != nil {
		return "", err
	}
	log.Infoln("Task passed to Mesos Scheduler")

	// Mesos is asynchronous, so we need to stage through the jobs. The
	// scheduler closes the channel when the job terminates, AFTER sending the
	// final update, so this loop exits naturally.
	for state := range respCh {
		// Use a status specific logger while we're listening here
		log := log.WithField("job_status", r.Job.Status)
		log.Infoln("mesos task status update:", state.State.String())

		// TODO: do we need to ensure tasks within a build schedule to the same node?

		switch status := state.GetState(); status {
		case mesos.TASK_STAGING:
			r.Job.Status = model.StatusPending
		case mesos.TASK_STARTING:
			r.Job.Status = model.StatusRunning
			r.Job.Started = time.Now().UTC().Unix()
		case mesos.TASK_RUNNING:
			r.Job.Status = model.StatusRunning
		case mesos.TASK_FINISHED:
			r.Job.Status = model.StatusSuccess
			r.Job.ExitCode = 0
			r.Job.Finished = time.Now().UTC().Unix()
		case mesos.TASK_FAILED:
			r.Job.Status = model.StatusFailure
			r.Job.ExitCode = 1
			r.Job.Finished = time.Now().UTC().Unix()
		case mesos.TASK_KILLED:
			r.Job.Status = model.StatusKilled
			r.Job.ExitCode = 1
			r.Job.Finished = time.Now().UTC().Unix()
		case mesos.TASK_LOST:
			// This means Mesos has lost the task and could indicate a slave
			// failure. Either way, we're done.
			r.Job.Status = model.StatusError
			r.Job.ExitCode = 1
			r.Job.Finished = time.Now().UTC().Unix()
		case mesos.TASK_ERROR:
			r.Job.Status = model.StatusError
			r.Job.ExitCode = 1
			r.Job.Finished = time.Now().UTC().Unix()
		}

		// update the task in the datastore
		err = Updater.SetJob(c, r)
		if err != nil {
			log.Errorf("error updating job status: %s", err)
			return slaveId, err
		}
	}

	log.Debugln("Scheduler closed task channel.")

	// TODO: collecting logs. We could read stdout/stderr, but it possibly
	// makes more sense to use either framework messages OR just register URLs
	// and do direct passing.

	//func() {
	//	logscli, err := mesoslogs.NewMesosClient(this.master)
	//	if err != nil {
	//		log.Errorln("Could not create mesos HTTP client to fetch logs:", err)
	//		return
	//	}
	//
	//	log.Debugln(spew.Sprintln(logscli.State))
	//
	//	logout, err := logscli.GetLog(mesosTask.GetName(), mesoslogs.STDOUT, "")
	//	if err != nil {
	//		log.Errorln("Could not retrieve logs for job:", err)
	//		return
	//	}
	//
	//	strReader := bytes.NewReader([]byte(logout[0].Log))
	//
	//	err = Updater.SetLogs(c, r, ioutil.NopCloser(strReader) )
	//}()

	// WAIT FOR OUTPUT
	// TODO: we need to tail slave logs here (see DCOS CLI)

	// TODO: I think we just need to do a direct download here?
	// send the logs to the datastore
	//var buf bytes.Buffer
	//rc, err := client.ContainerLogs(name, docker.LogOpts)
	//if err != nil && builderr != nil {
	//	buf.WriteString("Error launching build")
	//	buf.WriteString(builderr.Error())
	//} else if err != nil {
	//	buf.WriteString("Error launching build")
	//	buf.WriteString(err.Error())
	//	log.Errorf("error openig connection to logs. %s", err)
	//	return err
	//} else {
	//	defer rc.Close()
	//	stdcopy.StdCopy(&buf, &buf, io.LimitReader(rc, 5000000))
	//}

	//err = Updater.SetLogs(c, r, ioutil.NopCloser(&buf))
	//if err != nil {
	//	log.Errorf("error updating logs. %s", err)
	//	return err
	//}

	log.Infoln("completed job")
	return slaveId, err
}

func (this *mesosEngine) runJobNotify(r *Task, slaveId string) error {
	var err error
	log := log.WithField("build", r.Build.ID).WithField("job", "notify")

	name := fmt.Sprintf("drone_build_%d_notify", r.Build.ID)

	defer func() {
		this.scheduler.KillTask(mesos.TaskID{Value: name})
	}()

	// encode the build payload to write to stdin
	// when launching the build container
	in, err := EncodeToLegacyFormat(r)
	if err != nil {
		log.Errorf("failure to marshal work. %s", err)
		return err
	}

	args := DefaultNotifyArgs
	args = append(args, "--")
	args = append(args, string(in))

	conf := &dockerclient.ContainerConfig{
		Image:      DefaultAgent,
		Entrypoint: DefaultEntrypoint,
		Cmd:        args,
		Env:        this.envs.ToSlice(),
		HostConfig: dockerclient.HostConfig{
			Binds:            []string{"/var/run/docker.sock:/var/run/docker.sock"},
			MemorySwappiness: -1,
		},
		Volumes: map[string]struct{}{
			"/var/run/docker.sock": struct{}{},
		},
	}

	mesosTask := dockerToMesosTask(name, conf)
	log.Debugln("converted job to mesos task")

	respCh, err := this.scheduler.assignTaskToNode(mesosTask)
	if err != nil {
		log.Errorln("Error scheduling notification container")
		return err
	}

	// Mesos is asynchronous, so we need to stage through the jobs. The
	// scheduler closes the channel when the job terminates, AFTER sending the
	// final update, so this loop exits naturally.
	for state := range respCh {
		log.Infoln("Mesos Job Status:", state.State.String(), state.GetReason().String(), state.GetMessage())
		switch state.GetState() {
		case mesos.TASK_STAGING:
			r.Job.Status = model.StatusPending
		case mesos.TASK_STARTING:
			r.Job.Status = model.StatusRunning
		case mesos.TASK_RUNNING:
			r.Job.Status = model.StatusRunning
		case mesos.TASK_FINISHED:
			r.Job.Status = model.StatusSuccess
			r.Job.ExitCode = 0
			err = nil
		case mesos.TASK_FAILED:
			r.Job.Status = model.StatusFailure
			r.Job.ExitCode = 1
			err = errors.New("runJobNotify: Notification container failed")
		case mesos.TASK_KILLED:
			r.Job.Status = model.StatusKilled
			r.Job.ExitCode = 1
			err = errors.New("runJobNotify: Notification container killed")
		case mesos.TASK_LOST:
			// This means Mesos has lost the task and could indicate a slave
			// failure. Either way, we're done.
			r.Job.Status = model.StatusError
			r.Job.ExitCode = 1
			err = errors.New("runJobNotify: Notification container lost")
		case mesos.TASK_ERROR:
			r.Job.Status = model.StatusError
			r.Job.ExitCode = 1
			err = errors.New("runJobNotify: Notification container error")
		}
	}

	// for debugging purposes we print a failed notification executions
	// output to the logs. Otherwise we have no way to troubleshoot failed
	// notifications. This is temporary code until I've come up with
	// a better solution.
	//if info != nil && info.State.ExitCode != 0 && log.GetLevel() >= log.InfoLevel {
	//	var buf bytes.Buffer
	//	rc, err := client.ContainerLogs(name, docker.LogOpts)
	//	if err == nil {
	//		defer rc.Close()
	//		stdcopy.StdCopy(&buf, &buf, io.LimitReader(rc, 50000))
	//	}
	//	log.Infof("Notification container %s exited with %d", name, info.State.ExitCode)
	//	log.Infoln(buf.String())
	//}

	return err
}
