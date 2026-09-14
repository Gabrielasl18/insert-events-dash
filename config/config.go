package config

const (
	Directory = "./dash"
	Porta     = ":8083"

	SegmentDurationUs = 2000000

	EventScheme = "urn:test:event"
	EventValue  = "loop-events"

	MQTTBroker = "tcp://localhost:1883"
	MQTTTopic  = "video/event"

	DashMPD    = "http://localhost:8081/live/dash/playlist_1080p.mpd"
	EventsJSON = "http://localhost:8081/live/dash/events.json"
)
