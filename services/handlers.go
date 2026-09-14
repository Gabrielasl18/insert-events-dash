package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"tcc-teste/config"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	mqttClient mqtt.Client

	publishedEvents = make(map[string]bool)

	publishedMutex sync.Mutex
)

// ==============================
// MQTT
// ==============================

func InitMQTT() error {
	opts := mqtt.NewClientOptions()

	opts.AddBroker(config.MQTTBroker)

	opts.SetClientID(
		"tcc-teste-event-publisher",
	)

	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)

	mqttClient = mqtt.NewClient(opts)

	token := mqttClient.Connect()

	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

	fmt.Println(
		"✅ MQTT conectado:",
		config.MQTTBroker,
	)

	return nil
}

// ==============================
// Publica evento MQTT
// ==============================

func publishEventMQTT(
	eventType string,
	name string,
	description string,
	color []*int,
) {
	payload := map[string]interface{}{
		"type":        eventType,
		"name":        name,
		"description": description,
		"color":       color,
	}

	data, err := json.Marshal(payload)

	if err != nil {
		fmt.Println(
			"❌ Erro criando JSON MQTT:",
			err,
		)

		return
	}

	fmt.Println(
		"📤 MQTT:",
		string(data),
	)

	token := mqttClient.Publish(
		config.MQTTTopic,
		1,
		false,
		data,
	)

	if token.Wait() && token.Error() != nil {
		fmt.Println(
			"❌ Erro publicando MQTT:",
			token.Error(),
		)
	}
}

// ==============================
// Proxy do MPD remoto
// ==============================

func HandleMPD(
	w http.ResponseWriter,
	r *http.Request,
) {
	fmt.Println(
		"📡 Proxy MPD:",
		config.DashMPD,
	)

	resp, err := http.Get(config.DashMPD)

	if err != nil {
		http.Error(
			w,
			"erro buscando MPD remoto",
			http.StatusBadGateway,
		)

		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(
			w,
			fmt.Sprintf(
				"MPD retornou HTTP %d",
				resp.StatusCode,
			),
			http.StatusBadGateway,
		)

		return
	}

	contentType :=
		resp.Header.Get("Content-Type")

	if contentType == "" {
		contentType =
			"application/dash+xml"
	}

	w.Header().Set(
		"Content-Type",
		contentType,
	)

	w.Header().Set(
		"Cache-Control",
		"no-cache, no-store, must-revalidate",
	)

	w.Header().Set(
		"Pragma",
		"no-cache",
	)

	w.Header().Set(
		"Expires",
		"0",
	)

	if _, err := io.Copy(
		w,
		resp.Body,
	); err != nil {
		fmt.Println(
			"❌ Erro enviando MPD:",
			err,
		)
	}
}

// ==============================
// Gera eventos
// ==============================

func GerarEventos(
	segmentoGlobal int,
	segmentoRelativo int,
) {
	Mutex.RLock()

	events := Events

	Mutex.RUnlock()

	for _, ev := range events {
		if ev == nil {
			continue
		}

		start := intValue(ev.SegStart)
		end := intValue(ev.SegEnd)

		name := stringValue(ev.Name)
		desc := stringValue(ev.Desc)

		fmt.Printf(
			"🔎 evento=%s | start=%d | end=%d | atual=%d\n",
			name,
			start,
			end,
			segmentoRelativo,
		)

		// START
		if segmentoRelativo == start {
			eventID := fmt.Sprintf(
				"%s_start_loop_%d",
				name,
				segmentoGlobal,
			)

			publishOnce(
				eventID,
				"start",
				name,
				desc,
				ev.Color,
			)
		}

		// END
		if segmentoRelativo == end {
			eventID := fmt.Sprintf(
				"%s_end_loop_%d",
				name,
				segmentoGlobal,
			)

			publishOnce(
				eventID,
				"end",
				name,
				desc,
				ev.Color,
			)
		}
	}
}

// ==============================
// Evita publicação duplicada
// ==============================

func publishOnce(
	eventID string,
	eventType string,
	name string,
	description string,
	color []*int,
) {
	publishedMutex.Lock()

	if publishedEvents[eventID] {
		publishedMutex.Unlock()

		return
	}

	publishedEvents[eventID] = true

	publishedMutex.Unlock()

	fmt.Printf(
		"🚀 EVENTO %s | %s | segmento=%s\n",
		eventType,
		name,
		eventID,
	)

	publishEventMQTT(
		eventType,
		name,
		description,
		color,
	)
}
