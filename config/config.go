package config

const (
	Directory         = "./dash"
	StreamBaseURL     = "http://localhost:8081/live/dash"
	Porta             = ":8083"
	SegmentDurationUs = 2000000

	EventScheme = "urn:test:event"
	EventValue  = "loop-events"

	// MQTT
	MQTTBroker = "tcp://localhost:1883"
	MQTTTopic  = "video/event"
)
