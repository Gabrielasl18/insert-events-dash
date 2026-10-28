package services

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"go-events/config"
	"go-events/models"
	"net/http"
	"strings"
	"time"
)

type DASHSession struct {
	CurrentGlobalSegment int
	SegmentsPerLoop      int
	Events               []models.Evento
	ActiveEventKey       string
}

func NewDASHSession() *DASHSession {
	return &DASHSession{}
}

func FetchMPD(ctx context.Context, client *http.Client, mpdURL string) (*models.MPD, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mpdURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var mpd models.MPD
	return &mpd, xml.NewDecoder(resp.Body).Decode(&mpd)
}

func GetLatestChunk(mpd *models.MPD) (int, error) {
	var template models.SegmentTemplate
	for _, a := range mpd.Period.AdaptationSets {
		if a.ContentType != "" && a.ContentType != "video" {
			continue
		}
		for _, r := range a.Representations {
			t := r.SegmentTemplate
			if t.StartNumber == 0 {
				t = a.SegmentTemplate
			}
			if t.StartNumber > 0 && len(t.SegmentTimeline.Segments) > 0 {
				template = t
				break
			}
		}
	}

	if template.StartNumber == 0 {
		return 0, fmt.Errorf("[Error] SegmentTemplate não encontrado")
	}

	latest := template.StartNumber - 1
	for _, seg := range template.SegmentTimeline.Segments {
		if seg.Repeat < 0 {
			return 0, fmt.Errorf("[Error] r negativo não suportado")
		}
		latest += seg.Repeat + 1
	}
	return latest, nil
}

func eventKey(evt models.Evento) string {
	return fmt.Sprintf("%d:%d:%s:%s", evt.SegStart, evt.SegEnd, evt.Name, evt.Desc)
}

func (s *DASHSession) LoadEvents(mpd *models.MPD) error {
	stream := mpd.Period.EventStream
	events := make([]models.Evento, 0, len(stream.EventItems))

	for _, item := range stream.EventItems {
		var payload models.MPDEventPayload
		if json.Unmarshal([]byte(strings.TrimSpace(item.Data)), &payload) != nil {
			continue
		}

		start, end := payload.SegStart, payload.SegEnd
		if start == 0 {
			start = item.SegStart
		}
		if end == 0 {
			end = item.SegEnd
		}

		if start < 0 || end <= start {
			continue
		}

		events = append(events, models.Evento{
			SegStart: start, SegEnd: end,
			Name: payload.Action.Name, Desc: payload.Action.Description, Color: payload.Action.Color,
		})
	}

	if stream.LastSegment <= 0 {
		return fmt.Errorf("[Error] total de chunks não configurado")
	}

	s.Events, s.SegmentsPerLoop = events, stream.LastSegment
	return nil
}

// Publica com retained = true para a interface manter o estado atual fixo
func publishEvent(payload []byte) error {
	if config.MQTTClient == nil || !config.MQTTClient.IsConnected() {
		return fmt.Errorf("[Error] Falha ao publicar evento, MQTT desconectado")
	}
	token := config.MQTTClient.Publish("video/event", 1, true, payload)
	token.Wait()
	return token.Error()
}

func (s *DASHSession) checkAndPublishEvents(ctx context.Context, relativeSegment int) {
	var currentActiveEvent *models.Evento
	var currentActiveKey string

	// 1. Identifica o evento que engloba o chunk relativo atual
	for _, evt := range s.Events {
		if relativeSegment >= evt.SegStart && relativeSegment < evt.SegEnd {
			e := evt
			currentActiveEvent = &e
			currentActiveKey = eventKey(evt)
			break
		}
	}

	// 2. Transição de estado: Só altera quando muda o evento ativo
	if s.ActiveEventKey != currentActiveKey {
		s.ActiveEventKey = currentActiveKey

		if currentActiveEvent != nil {
			// Entrou em um evento -> Dispara payload e retém no MQTT
			payload, _ := json.Marshal(models.EventPayload{
				Name:        currentActiveEvent.Name,
				Description: currentActiveEvent.Desc,
				Color:       currentActiveEvent.Color,
				SegStart:    currentActiveEvent.SegStart,
				SegEnd:      currentActiveEvent.SegEnd,
			})

			if err := publishEvent(payload); err == nil {
				fmt.Printf("[MQTT] Evento na tela: %s (Chunk %d [%d, %d))\n",
					currentActiveEvent.Name, relativeSegment, currentActiveEvent.SegStart, currentActiveEvent.SegEnd)
			}
		} else {
			// O evento terminou -> Limpa a retenção no MQTT com payload vazio
			if err := publishEvent([]byte{}); err == nil {
				fmt.Printf("[MQTT] Evento finalizado no chunk %d\n", relativeSegment)
			}
		}
	}
}

func WatchSegmentsMPD(ctx context.Context, mpdURL string) {
	client := &http.Client{Timeout: 3 * time.Second}
	session := NewDASHSession()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	lastChunk := -1

	check := func() {
		mpd, err := FetchMPD(ctx, client, mpdURL)
		if err != nil || ctx.Err() != nil {
			return
		}

		if err := session.LoadEvents(mpd); err != nil {
			return
		}
		latestChunk, err := GetLatestChunk(mpd)
		if err != nil {
			return
		}

		if lastChunk >= 0 && latestChunk < lastChunk {
			session.ActiveEventKey = ""
			lastChunk = -1
		}

		relativeSegment := (latestChunk - 1) % session.SegmentsPerLoop
		session.CurrentGlobalSegment = latestChunk

		session.checkAndPublishEvents(ctx, relativeSegment)
		lastChunk = latestChunk
	}

	check()
	for {
		select {
		case <-ctx.Done():
			// Ao encerrar o monitoramento, limpa o estado retido no broker
			_ = publishEvent([]byte{})
			return
		case <-ticker.C:
			check()
		}
	}
}
