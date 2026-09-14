package services

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sync"
	"tcc-teste/config"
	"tcc-teste/models"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	mqttClient mqtt.Client

	publishedEvents      = make(map[string]bool)
	publishedEventsMutex = &sync.Mutex{}
)

// ============================================================
// MQTT
// ============================================================

func InitMQTT() error {
	opts := mqtt.NewClientOptions()

	opts.AddBroker(config.MQTTBroker)
	opts.SetClientID("tcc-teste-event-publisher")
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(false)

	client := mqtt.NewClient(opts)

	token := client.Connect()

	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

	mqttClient = client

	fmt.Println("=================================")
	fmt.Println("✅ MQTT conectado")
	fmt.Println("Broker:", config.MQTTBroker)
	fmt.Println("Topic:", config.MQTTTopic)
	fmt.Println("=================================")

	return nil
}

// ============================================================
// PUBLICAÇÃO MQTT
// ============================================================

func publishEventMQTT(eventID string, payload []byte) {

	fmt.Println("📤 TENTANDO PUBLICAR MQTT")
	fmt.Println("   ID:", eventID)
	fmt.Println("   Topic:", config.MQTTTopic)
	fmt.Println("   Payload:", string(payload))

	if mqttClient == nil {
		fmt.Println("❌ mqttClient é nil")
		return
	}

	if !mqttClient.IsConnected() {
		fmt.Println("❌ MQTT não está conectado")
		return
	}

	publishedEventsMutex.Lock()
	defer publishedEventsMutex.Unlock()

	// Evita publicar o mesmo evento duas vezes.
	if publishedEvents[eventID] {
		fmt.Println("⚠️ Evento já publicado:", eventID)
		return
	}

	token := mqttClient.Publish(
		config.MQTTTopic,
		1,     // QoS
		false, // retain
		payload,
	)

	token.Wait()

	if token.Error() != nil {
		fmt.Println("❌ Erro publicando evento MQTT:", token.Error())
		return
	}

	// Só marca depois que o Publish realmente funcionou.
	publishedEvents[eventID] = true

	fmt.Println("📡 EVENTO MQTT PUBLICADO")
	fmt.Println("   ID:", eventID)
	fmt.Println("   Topic:", config.MQTTTopic)
	fmt.Println("   Payload:", string(payload))
}

// ============================================================
// HANDLE MPD
// ============================================================

func HandleMPD(w http.ResponseWriter, r *http.Request) {

	fmt.Println("🔥 MPD REQUEST:", r.URL.Path)

	path := config.Directory + r.URL.Path

	data, err := os.ReadFile(path)
	if err != nil {
		http.Error(w, "MPD não encontrado", http.StatusNotFound)
		return
	}

	mpd := string(data)

	// ----------------------------------------------------------
	// Descobre o segmento atual
	// ----------------------------------------------------------

	Mutex.RLock()

	segmentoGlobal := SegmentoGlobalAtual

	Mutex.RUnlock()

	if segmentoGlobal == 0 {
		segmentoGlobal = 1
	}

	if SegmentosPorLoop <= 0 {
		fmt.Println("❌ SegmentosPorLoop inválido:", SegmentosPorLoop)
		http.Error(
			w,
			"SegmentosPorLoop inválido",
			http.StatusInternalServerError,
		)
		return
	}

	segmentoRelativo := (segmentoGlobal - 1) % SegmentosPorLoop

	fmt.Println(
		"➡️ MPD REQUEST | global:",
		segmentoGlobal,
		"| relativo:",
		segmentoRelativo,
	)

	// ==========================================================
	// EVENTOS MQTT
	// ==========================================================

	GerarEventos(segmentoGlobal, segmentoRelativo)

	// ==========================================================
	// PARTE ANTIGA — INJEÇÃO DO EVENTSTREAM NO MPD
	// ==========================================================
	//
	// Mantida comentada.
	//
	// eventsXML := GerarEventos(...)
	//
	// if eventsXML != "" {
	//     mpd = strings.Replace(
	//         mpd,
	//         "</Period>",
	//         "\n"+eventsXML+"\n</Period>",
	//         1,
	//     )
	//
	//     pathModify := config.Directory + "/modify" + r.URL.Path
	//
	//     err := os.WriteFile(
	//         pathModify,
	//         []byte(mpd),
	//         0644,
	//     )
	//
	//     if err != nil {
	//         fmt.Println("erro salvando MPD:", err)
	//     }
	// }
	//
	// ==========================================================

	// Atualiza publishTime.
	now := time.Now().UTC().Format(time.RFC3339Nano)

	rePublish := regexp.MustCompile(`publishTime="[^"]+"`)

	mpd = rePublish.ReplaceAllString(
		mpd,
		`publishTime="`+now+`"`,
	)

	// Atualiza minimumUpdatePeriod.
	reMUP := regexp.MustCompile(`minimumUpdatePeriod="[^"]+"`)

	mpd = reMUP.ReplaceAllString(
		mpd,
		`minimumUpdatePeriod="PT1S"`,
	)

	fmt.Println("========== MPD ENVIADO ==========")
	fmt.Println(mpd)
	fmt.Println("=================================")

	w.Header().Set(
		"Content-Type",
		"application/dash+xml",
	)

	_, _ = w.Write([]byte(mpd))
}

// ============================================================
// GERA EVENTOS
// ============================================================

func GerarEventos(
	segmentoGlobal int,
	segmentoRelativo int,
) string {

	fmt.Println("=================================")
	fmt.Println("🔎 VERIFICANDO EVENTOS")
	fmt.Println("   global:", segmentoGlobal)
	fmt.Println("   relativo:", segmentoRelativo)
	fmt.Println("   total:", len(Events))
	fmt.Println("=================================")

	// ==========================================================
	// EVENTSTREAM ANTIGO
	// ==========================================================

	stream := models.EventStream{
		XMLName:   xml.Name{Local: "EventStream"},
		SchemeID:  config.EventScheme,
		Value:     config.EventValue,
		Timescale: 1000000,
	}

	for _, ev := range Events {

		fmt.Printf(
			"🔎 evento=%s | start=%d | end=%d | atual=%d\n",
			ev.Name,
			ev.SegStart,
			ev.SegEnd,
			segmentoRelativo,
		)

		// ======================================================
		// EVENTO START
		// ======================================================

		if segmentoRelativo == ev.SegStart {

			payload, err := json.Marshal(map[string]any{
				"type":        "start",
				"name":        ev.Name,
				"description": ev.Desc,
				"color":       ev.Color,
			})

			if err != nil {
				fmt.Println(
					"❌ Erro criando payload START:",
					err,
				)
				continue
			}

			fmt.Println("🚀 ENCONTROU START:", ev.Name)

			eventID := fmt.Sprintf(
				"%s_start_loop_%d",
				ev.Name,
				segmentoGlobal,
			)

			publishEventMQTT(
				eventID,
				payload,
			)

			// --------------------------------------------------
			// PARTE ANTIGA — EVENTSTREAM XML
			// --------------------------------------------------

			stream.EventItems = append(
				stream.EventItems,
				models.Event{
					XMLName: xml.Name{
						Local: "Event",
					},

					PresentationTime: int64(
						segmentoGlobal-1,
					) * config.SegmentDurationUs,

					Duration: config.SegmentDurationUs,

					ID: fmt.Sprintf(
						"%s_start_loop",
						ev.Name,
					),

					Data: string(payload),
				},
			)
		}

		// ======================================================
		// EVENTO END
		// ======================================================

		if segmentoRelativo == ev.SegEnd {

			payload, err := json.Marshal(map[string]any{
				"type":        "end",
				"name":        ev.Name,
				"description": ev.Desc,
				"color":       ev.Color,
			})

			if err != nil {
				fmt.Println(
					"❌ Erro criando payload END:",
					err,
				)
				continue
			}

			fmt.Println("🛑 ENCONTROU END:", ev.Name)

			eventID := fmt.Sprintf(
				"%s_end_loop_%d",
				ev.Name,
				segmentoGlobal,
			)

			publishEventMQTT(
				eventID,
				payload,
			)

			// --------------------------------------------------
			// PARTE ANTIGA — EVENTSTREAM XML
			// --------------------------------------------------

			stream.EventItems = append(
				stream.EventItems,
				models.Event{
					XMLName: xml.Name{
						Local: "Event",
					},

					PresentationTime: int64(
						segmentoGlobal-1,
					) * config.SegmentDurationUs,

					Duration: config.SegmentDurationUs,

					ID: fmt.Sprintf(
						"%s_end_loop",
						ev.Name,
					),

					Data: string(payload),
				},
			)
		}
	}

	// ==========================================================
	// PARTE ANTIGA — SERIALIZAÇÃO XML
	// ==========================================================

	if len(stream.EventItems) == 0 {
		fmt.Println("ℹ️ Nenhum evento neste segmento")
		fmt.Println("=================================")
		return ""
	}

	output, err := xml.MarshalIndent(
		stream,
		"    ",
		"    ",
	)

	if err != nil {
		fmt.Println(
			"❌ Erro serializando EventStream:",
			err,
		)
		return ""
	}

	fmt.Println("=================================")

	return string(output)
}
