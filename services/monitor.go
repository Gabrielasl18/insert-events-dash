package services

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"tcc-teste/config"
	"tcc-teste/models"
	"time"
)

var (
	Mutex sync.RWMutex

	SegmentoGlobalAtual int
	SegmentosPorLoop    int
	Events              []models.Evento
	ActiveEvents        map[int]bool = make(map[int]bool)
)

// FetchAndParseMPD baixa o MPD via HTTP e extrai os eventos do <EventStream>
// FetchAndParseMPD baixa o MPD via HTTP e extrai os eventos do <EventStream>
func FetchAndParseMPD(mpdURL string) error {
	// 1. Busca o arquivo MPD via HTTP
	resp, err := http.Get(mpdURL)
	if err != nil {
		return fmt.Errorf("erro HTTP ao buscar MPD da emissora: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("emissora respondeu com status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("erro ao ler o corpo do MPD: %w", err)
	}

	// 2. Parse do XML do MPD
	var mpd models.MPD
	if err := xml.Unmarshal(body, &mpd); err != nil {
		return fmt.Errorf("erro no parse XML do MPD: %w", err)
	}

	// 3. Determina a quantidade de segmentos por loop
	segmentosLoop := 60
	if mpd.Period.EventStream.MaxSegment > 0 {
		segmentosLoop = mpd.Period.EventStream.MaxSegment
	} else if mpd.Period.EventStream.LastSegment > 0 {
		segmentosLoop = mpd.Period.EventStream.LastSegment
	}

	// 4. Processa os eventos em memória local antes de atualizar o estado global
	var parsedEvents []models.Evento

	for _, e := range mpd.Period.EventStream.EventItems {
		rawPayload := strings.TrimSpace(e.Data)
		if rawPayload == "" {
			continue
		}

		// Estratégia A: Formato estruturado (Envelope models.MPDEventPayload)
		var payload models.MPDEventPayload
		if err := json.Unmarshal([]byte(rawPayload), &payload); err == nil && (payload.Action.Name != "" || len(payload.Action.Color) > 0) {
			parsedEvents = append(parsedEvents, models.Evento{
				SegStart: payload.SegStart,
				SegEnd:   payload.SegEnd,
				Name:     payload.Action.Name,
				Desc:     payload.Action.Description,
				Color:    payload.Action.Color,
			})
			continue
		}

		// Estratégia B: Mapa genérico dinâmico
		var genericMap map[string]interface{}
		if err := json.Unmarshal([]byte(rawPayload), &genericMap); err == nil {
			evento := parseGenericEventMap(genericMap, e.SegStart, e.SegEnd)
			parsedEvents = append(parsedEvents, evento)
		}
	}

	// 5. Atualização segura do estado global protegida pelo Mutex
	Mutex.Lock()
	SegmentosPorLoop = segmentosLoop
	Events = parsedEvents
	Mutex.Unlock()

	fmt.Printf("✅ [AoP] %d eventos extraídos com sucesso do EventStream do MPD!\n", len(parsedEvents))
	fmt.Printf("✅ [AoP] Segmentos por Loop = %d\n", segmentosLoop)
	return nil
}

// Helper para extrair atributos de um mapa genérico de eventos
func parseGenericEventMap(genericMap map[string]interface{}, defaultSegStart, defaultSegEnd int) models.Evento {
	var name, desc string
	var color []int

	// Se existir o objeto 'action', foca nele. Caso contrário, lê da raiz do mapa.
	targetMap := genericMap
	if actionObj, ok := genericMap["action"].(map[string]interface{}); ok {
		targetMap = actionObj
	}

	// Extrai Name e Description
	if n, ok := targetMap["name"].(string); ok {
		name = n
	}
	if d, ok := targetMap["description"].(string); ok {
		desc = d
	}

	// Extrai vetor de cores
	if cols, ok := targetMap["color"].([]interface{}); ok {
		for _, c := range cols {
			switch val := c.(type) {
			case float64:
				color = append(color, int(val))
			case int:
				color = append(color, val)
			}
		}
	}

	// Extrai limites de segmento se fornecidos no próprio JSON
	segStart := defaultSegStart
	segEnd := defaultSegEnd

	if s, ok := genericMap["seg_start"].(float64); ok {
		segStart = int(s)
	}
	if s, ok := genericMap["seg_end"].(float64); ok {
		segEnd = int(s)
	}

	return models.Evento{
		SegStart: segStart,
		SegEnd:   segEnd,
		Name:     name,
		Desc:     desc,
		Color:    color,
	}
}

// WatchSegmentsHTTP monitora a disponibilidade dos chunks .m4s fazendo polling no servidor Express
// Defina quantos segmentos deseja antecipar para compensar o buffer do player (ex: 1 ou 2)
func WatchSegmentsHTTP(baseURL string) {
	currentSegment := 1
	client := &http.Client{Timeout: 1 * time.Second} // Reduzido o timeout para 1s

	fmt.Println("👀 AoP Monitorando requisições de chunks no servidor via HTTP...")

	for {
		formats := []string{
			fmt.Sprintf("chunk-stream0-%05d.m4s", currentSegment),
			fmt.Sprintf("chunk-stream0-%d.m4s", currentSegment),
			fmt.Sprintf("chunk-stream0-%03d.m4s", currentSegment),
		}

		found := false

		for _, chunkName := range formats {
			chunkURL := fmt.Sprintf("%s/%s", baseURL, chunkName)

			resp, err := client.Head(chunkURL)
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()

				Mutex.Lock()
				SegmentoGlobalAtual = currentSegment

				if SegmentosPorLoop <= 0 {
					SegmentosPorLoop = 60
				}

				// Aplica o offset de antecipação para compensar o delay
				segmentoCalculado := currentSegment
				segmentoRelativo := (segmentoCalculado - 1) % SegmentosPorLoop

				fmt.Printf("📦 [HTTP Client] Chunk: %s | 🔁 Relativo: %d\n",
					chunkName, segmentoRelativo)

				// Dispara os eventos calculados com antecipação
				checkAndPublishEvents(segmentoRelativo)
				Mutex.Unlock()

				currentSegment++
				found = true
				break
			}

			if resp != nil {
				resp.Body.Close()
			}
		}

		if !found {
			// Reduzido de 500ms para 100ms para reagir muito mais rápido assim que o chunk surge
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// checkAndPublishEvents efetua o disparo do evento no Broker MQTT (Camada AoP)
func checkAndPublishEvents(segmentoRelativo int) {
	for i, evt := range Events {
		if segmentoRelativo >= evt.SegStart && segmentoRelativo <= evt.SegEnd {
			if !ActiveEvents[i] {
				ActiveEvents[i] = true

				payloadData := models.EventPayload{
					Name:        evt.Name,
					Description: evt.Desc,
					Color:       evt.Color,
					SegStart:    evt.SegStart,
					SegEnd:      evt.SegEnd,
				}

				payload, err := json.Marshal(payloadData)
				if err != nil {
					fmt.Printf("❌ [AoP -> MQTT] Erro ao serializar JSON: %v\n", err)
					continue
				}

				topic := "video/event"
				fmt.Printf("🚀 [AoP -> MQTT] Disparando evento no segmento %d (Tópico: %s): %s\n", segmentoRelativo, topic, string(payload))

				// Publica no tópico video/event no Broker MQTT
				if config.MQTTClient != nil && config.MQTTClient.IsConnected() {
					token := config.MQTTClient.Publish(topic, 0, false, payload)
					token.Wait()
					if token.Error() != nil {
						fmt.Printf("❌ [AoP -> MQTT] Erro ao publicar no tópico %s: %v\n", topic, token.Error())
					} else {
						fmt.Printf("✅ [AoP -> MQTT] Publicado com sucesso no tópico '%s'!\n", topic)
					}
				} else {
					fmt.Println("⚠️ [AoP -> MQTT] Cliente MQTT não está conectado!")
				}
			}
		} else {
			delete(ActiveEvents, i)
		}
	}
}
