package qtypes_messages

import (
	"encoding/json"
	"github.com/qframe/types/plugin"
	"fmt"
)

type Message struct {
	Base
	Message string
}

func NewMessage(b Base, msg string) Message {
	m := Message{
		Base: b,
		Message: msg,
	}
	m.GenDefaultID()
	return m
}

func (m *Message) ParseJsonMap(p *qtypes_plugin.Plugin, kv map[string]string) {
	for _, txt := range kv {
		p.Log("debug", fmt.Sprintf("txt to parse to json: %s", txt))
		byt := []byte(txt)
		var dat map[string]interface{}
		json.Unmarshal(byt, &dat)
		for k, v := range dat {
			if _, ok := m.Tags[k]; !ok {
				p.Log("debug", fmt.Sprintf("New key in tag '%s' for message '%s'", k, txt))
				m.Tags[k] = fmt.Sprintf("%s", v)
			} else {
				p.Log("debug", fmt.Sprintf("Overwrite tag '%s' in message '%s'", k, txt))
				m.Tags[k] = fmt.Sprintf("%s", v)
			}
		}
	}

}