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

// InitMQTT configura e conecta o cliente MQTT centralizado da aplicação.
func InitMQTT(brokerURL string, clientID string, onConnect mqtt.OnConnectHandler) error {
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetConnectRetry(true)

	if onConnect != nil {
		opts.SetOnConnectHandler(onConnect)
	}

	MQTTClient = mqtt.NewClient(opts)

	token := MQTTClient.Connect()
	token.Wait()

	if err := token.Error(); err != nil {
		return fmt.Errorf("falha ao conectar ao MQTT: %w", err)
	}

	return nil
}
