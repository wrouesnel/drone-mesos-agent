package mesos_engine

import (
	//util "github.com/mesos/mesos-go/mesosutil"
	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go"
	"github.com/samalba/dockerclient"
	"strings"
)

// Utility to convert a dockerclient libary to a mesos task. This exists
// pretty much because Mesos task notation in Go is very ugly.
func dockerToMesosTask(name string, cfg *dockerclient.ContainerConfig) mesos.TaskInfo {
	// Convert docker volumes configs to mesos volume configs
	volumes := []mesos.Volume{}
	for _, vol := range cfg.HostConfig.Binds {
		containerOpts := strings.Split(vol, ":")

		var volumeOpt *mesos.Volume_Mode
		if len(containerOpts) >= 3 {
			switch containerOpts[2] {
			case "rw":
				volumeOpt = mesos.RW.Enum()
			case "ro":
				volumeOpt = mesos.RO.Enum()
			default:
				volumeOpt = mesos.RW.Enum()
			}
		} else {
			volumeOpt = mesos.RW.Enum()
		}

		var containerPath string
		var hostPath *string

		if len(containerOpts) == 2 {
			containerPath = containerOpts[1]
			hostPath = &containerOpts[0]
		} else {
			containerPath = vol
		}

		mesosVol := mesos.Volume{
			Mode:          volumeOpt,
			ContainerPath: containerPath,
			HostPath:      hostPath,
		}
		volumes = append(volumes, mesosVol)
	}

	mesosTask := mesos.TaskInfo{
		TaskID: mesos.TaskID{Value: name},
		Name:   name,
		// These need to be set
		//Resources: []*mesos.Resource{
		//	// TODO: we need to edit the data model to support resource limiting
		//	util.NewScalarResource("cpus", 1),
		//	util.NewScalarResource("mem", 128),
		//	util.NewScalarResource("disk", 500),
		//},
		Container: &mesos.ContainerInfo{
			Type:    mesos.ContainerInfo_DOCKER.Enum(),
			Volumes: volumes,
			Docker: &mesos.ContainerInfo_DockerInfo{
				Image: cfg.Image,
				Parameters: []mesos.Parameter{
					mesos.Parameter{"entrypoint", strings.Join(cfg.Entrypoint, " ")},
				},
				ForcePullImage: proto.Bool(false), // TODO: its not clear what we want to do with this
			},
		},
		Command: &mesos.CommandInfo{
			Shell:     proto.Bool(false),
			Arguments: cfg.Cmd,
			User:      proto.String("root"),
		},
	}

	return mesosTask
}

// Helper to get a URL query parameter with a default value. Used to populate
// Mesos structs from engine params. Should probably be made a library.
//func getQueryParam(u *url.URL, name string, def string) string {
//	r := u.Query().Get(name)
//	if r != "" {
//		return r
//	}
//	return def
//}
//
//// Same as get query param but parses and returns a boolean value fairly loosely
//func getQueryParamBool(u *url.URL, name string, def bool) bool {
//	r := u.Query().Get(name)
//	r = strings.ToLower(r)
//	switch r {
//	case "yes", "true", "1":
//		return true
//	case "no", "false", "0":
//		return false
//	default: return def
//	}
//}
//
//func getQueryParamInt(u *url.URL, name string, def string) int {
//	r := u.Query().Get(name)
//	if r != "" {
//		return r
//	}
//	return def
//}
