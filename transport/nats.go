package transport

import (
	flowmessage "github.com/cloudflare/goflow/pb"
	"github.com/cloudflare/goflow/utils"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
)

type NATSState struct {
	connection *nats.Conn
	log        utils.Logger
}

func StartNATSProducerFromArgs(log utils.Logger) (*NATSState, error) {
	return StartNATSProducer(log)
}

func StartNATSProducer(log utils.Logger) (*NATSState, error) {
	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return nil, err
	}

	state := NATSState{
		connection: conn,
		log:        log,
	}

	return &state, nil
}

func (s NATSState) Publish(msgs []*flowmessage.FlowMessage) {
	for _, msg := range msgs {
		if buf, err := proto.Marshal(msg); err == nil {
			if err := s.connection.Publish("goflow", buf); err != nil {
				s.log.Errorf("NATS publish error: %v", err)
			}
		} else {
			s.log.Errorf("Protobuf Marshal error: %v", err)
		}
	}
}
