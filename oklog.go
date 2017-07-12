package oklog

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/gliderlabs/logspout/router"
)

var hostname string

func init() {
	hostname, _ = os.Hostname()
	router.AdapterFactories.Register(NewOklogAdapter, "oklog")
}

// OklogAdapter is an adapter that streams UDP JSON to Graylog
type OklogAdapter struct {
	writer net.Conn
	route  *router.Route
}

// NewOklogAdapter creates a OklogAdapter with UDP as the default transport.
func NewOklogAdapter(route *router.Route) (router.LogAdapter, error) {
	_, found := router.AdapterTransports.Lookup(route.AdapterTransport("tcp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}
	/*
		host, port, err := net.SplitHostPort(route.Address)
		if err != nil {
			return nil, err
		}
	*/
	log.Printf("oklog: connect to %s", route.Address)
	conn, err := net.Dial("tcp", route.Address)
	if err != nil {
		return nil, err
	}
	return &OklogAdapter{
		route:  route,
		writer: conn,
	}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *OklogAdapter) Stream(logstream chan *router.Message) {
	for message := range logstream {
		m := &OklogMessage{message}
		extra, err := m.getExtraFields()
		if err != nil {
			log.Println("oklog:", err)
			continue
		}

		msg := RawOklogMessage{
			Version:  "1.0",
			Host:     hostname,
			Short:    m.Message.Data,
			TimeUnix: float64(m.Message.Time.UnixNano()/int64(time.Millisecond)) / 1000.0,
			RawExtra: extra,
		}
		// 	ContainerId:    m.Container.ID,
		// 	ContainerImage: m.Container.Config.Image,
		// 	ContainerName:  m.Container.Name,
		// }

		// here be message write.
		buf, err := json.Marshal(&msg)
		if err != nil {
			log.Println("oklog:", err)
			continue
		}
		log.Printf("oklog: writing %s", buf)
		if _, err := a.writer.Write(append(buf, byte('\n'))); err != nil {
			log.Println("oklog:", err)
			continue
		}
	}
}

type RawOklogMessage struct {
	Version  string          `json:"version"`
	Host     string          `json:"host"`
	Short    string          `json:"short"`
	TimeUnix float64         `json:"timeunix"`
	RawExtra json.RawMessage `json:"extra,omitempty"`
}

type OklogMessage struct {
	*router.Message
}

func (m OklogMessage) getExtraFields() (json.RawMessage, error) {

	extra := map[string]interface{}{
		"_container_id":   m.Container.ID,
		"_container_name": m.Container.Name[1:], // might be better to use strings.TrimLeft() to remove the first /
		"_image_id":       m.Container.Image,
		"_image_name":     m.Container.Config.Image,
		"_command":        strings.Join(m.Container.Config.Cmd[:], " "),
		"_created":        m.Container.Created,
	}
	for name, label := range m.Container.Config.Labels {
		if strings.ToLower(name[0:5]) == "gelf_" {
			extra[name[4:]] = label
		}
	}
	swarmnode := m.Container.Node
	if swarmnode != nil {
		extra["_swarm_node"] = swarmnode.Name
	}

	rawExtra, err := json.Marshal(extra)
	if err != nil {
		return nil, err
	}
	return rawExtra, nil
}
