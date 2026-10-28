package config

import (
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	Directory         = "./dash"
	Porta             = ":8083"
	SegmentDurationUs = 2000000
	EventScheme       = "urn:test:event"
	EventValue        = "loop-events"
)

var (
	MQTTClient mqtt.Client
)

// InitMQTT conecta a aplicação em Go ao Broker MQTT para publicar os eventos
func InitMQTT(brokerURL string, clientID string) error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(clientID)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("falha ao conectar ao MQTT: %v", token.Error())
	}

	MQTTClient = client
	return nil
}
