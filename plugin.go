package qfilter_metrics

import (
	"fmt"
	"time"
	"strings"
	"strconv"
	"github.com/zpatrick/go-config"

	"github.com/qframe/types/messages"
	"github.com/qframe/types/plugin"
	"github.com/qframe/types/qchannel"
	"github.com/qframe/types/constants"
	"github.com/qframe/types/metrics"
	"reflect"
	"github.com/docker/docker/api/types"
)

const (
	version   = "0.2.7"
	pluginTyp = qtypes_constants.FILTER
	pluginPkg = "metric"
)

var (
	containerStates = map[string]float64{
		"create": 1.0,
		"start": 2.0,
		"healthy": 3.0,
		"unhealthy": 4.0,
		"kill": -1.0,
		"die": -2.0,
		"stop": -3.0,
		"destroy": -4.0,
	}
)

type Plugin struct {
	*qtypes_plugin.Plugin
}

func New(qChan qtypes_qchannel.QChan, cfg *config.Config, name string) (p Plugin, err error) {
	p = Plugin{
		Plugin: qtypes_plugin.NewNamedPlugin(qChan, cfg, pluginTyp, pluginPkg, name, version),
	}
	return
}

// Run fetches everything from the Data channel and flushes it to stdout
func (p *Plugin) Run() {
	p.Log("notice", fmt.Sprintf("Start plugin v%s", p.Version))
	rewriteDims := strings.Split(p.CfgStringOr("rewrite-dimensions", ""), ",")
	limitDims := strings.Split(p.CfgStringOr("limit-dimensions", ""), ",")
	rwDims := map[string]string{}
	for _, elem := range rewriteDims {
		kv := strings.Split(elem, ":")
		if len(kv) == 2 {
			rwDims[kv[0]] = kv[1]
		} else {
			p.Log("warn", fmt.Sprintf("Could not split rwDim '%s' by ':'", elem))
		}
	}
	ignoreContainerEvents := p.CfgBoolOr("ignore-container-events", true)
	dc := p.QChan.Data.Join()
	for {
		select {
		case val := <-dc.Read:
			switch val.(type) {
			case qtypes_messages.Message:
				msg := val.(qtypes_messages.Message)
				if msg.StopProcessing(p.Plugin, false) {
					continue
				}
				name, nok := msg.Tags["name"]
				tval, tok := msg.Tags["time"]
				value, vok := msg.Tags["value"]
				if nok && tok && vok {
					mval, err := strconv.ParseFloat(value, 64)
					if err != nil {
						p.Log("error", fmt.Sprintf("Unable to parse value '%s' as float: %s", value, err.Error()))
						continue
					}
					tint, err := strconv.Atoi(tval)
					if err != nil {
						p.Log("error", fmt.Sprintf("Unable to parse timestamp '%s' as int: %s", tint, err.Error()))
						continue
					}
					dims := map[string]string{}
					dims["source"] = msg.GetLastSource()
					met := qtypes_metrics.NewExt(p.Name, name, qtypes_metrics.Gauge, mval, dims, time.Unix(int64(tint), 0), true)
					tags, tagok := msg.Tags["tags"]
					if tagok {
						for _, item := range strings.Split(tags, ",") {
							dim := strings.Split(item, "=")
							if len(dim) == 2 {
								met.Dimensions[dim[0]] = dim[1]
							}
						}
					}
					p.Log("trace", "send metric")
					p.QChan.Data.Send(met)
				}
			case qtypes_messages.ContainerMessage:
				msg := val.(qtypes_messages.ContainerMessage)
				if msg.StopProcessing(p.Plugin, false) {
					continue
				}
				name, nok := msg.Tags["name"]
				tval, tok := msg.Tags["time"]
				value, vok := msg.Tags["value"]
				if nok && tok && vok {
					mval, err := strconv.ParseFloat(value, 64)
					if err != nil {
						p.Log("error", fmt.Sprintf("Unable to parse value '%s' as float: %s", value, err.Error()))
						continue
					}
					tint, err := strconv.Atoi(tval)
					if err != nil {
						p.Log("error", fmt.Sprintf("Unable to parse timestamp '%s' as int: %s", tint, err.Error()))
						continue
					}
					dims := AssembleJSONDefaultDimensions(&msg.Container)
					dims = AddEngineDims(dims, &msg.Engine)
					dims = RewriteDims(rwDims, dims)
					dims["source"] = msg.GetLastSource()
					tags, tagok := msg.Tags["tags"]
					if tagok {
						for _, item := range strings.Split(tags, " ") {
							dim := strings.Split(item, "=")
							if len(dim) == 2 {
								dims[dim[0]] = dim[1]
							}
						}
					}
					// TODO: Iterating yet again over the dimensions, not very efficient
					p.Log("debug", fmt.Sprintf("Len(limitDims)=%d | %v", len(limitDims), limitDims))
					endDims := map[string]string{}
					if len(limitDims) > 0 {
						for k, v := range dims {
							for _, allowKey := range limitDims {
								if allowKey == k {
									endDims[k] = v
									break
								}
							}
						}
					} else {
						endDims = dims
					}
					met := qtypes_metrics.NewExt(p.Name, name, qtypes_metrics.Gauge, mval, endDims, time.Unix(int64(tint), 0), true)
					p.Log("trace", "send metric")
					p.QChan.Data.Send(met)
				}
			default:
				if ignoreContainerEvents {
					continue
				}
				p.Log("trace", fmt.Sprintf("Dunno how to handle type: %s", reflect.TypeOf(val)))
			}
		}
	}
}

// AddEngineDims annotates engine information.
func AddEngineDims(dims map[string]string, eng *types.Info) map[string]string {
	res := map[string]string{
		"engine_name": eng.Name,
		"engine_kernel": eng.KernelVersion,
		"engine_address": eng.Swarm.NodeAddr,
		"engine_version": eng.ServerVersion,
	}
	for k,v := range dims {
		res[k] = v
	}
	return res
}

func RewriteDims(rw map[string]string, dims map[string]string) map[string]string {
	res := map[string]string{}
	for k,v := range dims {
		rewritten := false
		for oKey, nKey := range rw {
			if oKey == k {
				res[nKey] = v
				rewritten = true
				break
			}
		}
		if ! rewritten {
			res[k] = v
		}
	}
	return res
}

// AssembleServiceSlot create {{.Service.Name}}.{{.Task.Slot}}
func AssembleJSONServiceSlot(cnt *types.ContainerJSON) string {
	if tn, tnok := cnt.Config.Labels["com.docker.swarm.task.name"]; tnok {
		arr := strings.Split(tn, ".")
		if len(arr) != 3 {
			return "<nil>"
		}
		return fmt.Sprintf("%s.%s", arr[0], arr[1])
	}
	return "<nil>"
}

// AssembleServiceSlot create {{.Service.Name}}.{{.Task.Slot}}
func AssembleJSONTaskSlot(cnt *types.ContainerJSON) string {
	if tn, tnok := cnt.Config.Labels["com.docker.swarm.task.name"]; tnok {
		arr := strings.Split(tn, ".")
		if len(arr) != 3 {
			return "<nil>"
		}
		return arr[1]
	}
	return "<nil>"
}


// AssembleDefaultDimensions derives a set of dimensions from the container information.
func AssembleJSONDefaultDimensions(cnt *types.ContainerJSON) map[string]string {
	dims := map[string]string{
		"service_namespace": cnt.Config.Labels["com.docker.stack.namespace"],
		"container_id":   cnt.ID,
		"container_name": strings.Trim(cnt.Name, "/"),
		"image_name":     cnt.Image,
		"service_slot":   AssembleJSONServiceSlot(cnt),
		"task_slot":   	  AssembleJSONTaskSlot(cnt),
		"command":        strings.Replace(strings.Join(cnt.Config.Cmd, "#"), " ", "#", -1),
		"created":        cnt.Created,
	}
	//TODO: When exensivly using labels, this hurts
	/*for k, v := range cnt.Config.Labels {
		dv := strings.Replace(v, " ", "#", -1)
		dv = strings.Replace(v, ".", "_", -1)
		dims[k] = dv
	}*/
	return dims
}

/*func (p *Plugin) handleContainerEvent(ce qtypes_docker_events.ContainerEvent) {
	if strings.HasPrefix(ce.Event.Action, "exec_") {
		p.MsgCount["execEvent"]++
		return
	}
	action := ce.Event.Action
	if strings.HasPrefix(ce.Event.Action, "health_status") {
		action = strings.Split(ce.Event.Action, ":")[1]
	}
	action = strings.Trim(action, " ")
	dims := qtypes.AssembleJSONDefaultDimensions(&ce.Container)
	dims["state-type"] = "container"
	p.Log("info", fmt.Sprintf("Action '%s' by %v", action, dims))
	if mval, ok := containerStates[action]; ok {
		met := qtypes.NewExt(p.Name, "state", qtypes.Gauge, mval, dims, ce.Time, false)
		p.QChan.Data.Send(met)
	} else {
		p.Log("warn", fmt.Sprintf("Could not fetch '%s' from containerState", action))
		met := qtypes.NewExt(p.Name, "state", qtypes.Gauge, 0.0, dims, ce.Time, false)
		p.QChan.Data.Send(met)
	}
}
*/