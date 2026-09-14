package services

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"tcc-teste/config"
	"tcc-teste/models"
	"time"
)

var (
	Mutex *sync.RWMutex

	SegmentoGlobalAtual  int
	UltimoSegmentoFisico int
	SegmentosPorLoop     int

	Events []*models.Evento
)

// ============================================================
// CARREGA EVENTS.JSON
// ============================================================

func ReadAndMarshalFile(path string) error {

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var file models.EventsFile

	if err := json.Unmarshal(data, &file); err != nil {
		return err
	}

	SegmentosPorLoop = file.LastSeg + 1

	Events = nil

	for _, item := range file.Actions {

		Events = append(
			Events,
			&models.Evento{
				SegStart: item.SegStart,
				SegEnd:   item.SegEnd,
				Name:     item.Action.Name,
				Desc:     item.Action.Description,
				Color:    item.Action.Color,
			},
		)
	}

	fmt.Printf(
		"✅ %d Events loaded\n",
		len(Events),
	)

	fmt.Printf(
		"✅ segmentosPorLoop = %d\n",
		SegmentosPorLoop,
	)

	for _, ev := range Events {

		fmt.Printf(
			"EVENTO | name=%s | start=%d | end=%d\n",
			ev.Name,
			ev.SegStart,
			ev.SegEnd,
		)
	}

	return nil
}

// ============================================================
// MONITORAMENTO DOS SEGMENTOS
// ============================================================
func WatchSegments() {

	if Mutex == nil {
		Mutex = &sync.RWMutex{}
	}

	fmt.Println("👀 Monitorando segmentos calculados via MPD Live...")

	client := http.Client{Timeout: 2 * time.Second}
	targetURL := config.StreamBaseURL + "/playlist_1080p.mpd"

	// Expressões para capturar os campos de tempo e duração do MPD
	rePublish := regexp.MustCompile(`publishTime="([^"]+)"`)
	reStart := regexp.MustCompile(`availabilityStartTime="([^"]+)"`)
	reDuration := regexp.MustCompile(`duration="(\d+)"`)
	reTimescale := regexp.MustCompile(`timescale="(\d+)"`)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {

		resp, err := client.Get(targetURL)
		if err != nil {
			fmt.Println("❌ Erro no polling HTTP:", err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil || resp.StatusCode != http.StatusOK {
			continue
		}

		mpdContent := string(body)

		// 1. Extrai datas do MPD
		matchPublish := rePublish.FindStringSubmatch(mpdContent)
		matchStart := reStart.FindStringSubmatch(mpdContent)
		matchDuration := reDuration.FindStringSubmatch(mpdContent)
		matchTimescale := reTimescale.FindStringSubmatch(mpdContent)

		if len(matchPublish) < 2 || len(matchStart) < 2 || len(matchDuration) < 2 || len(matchTimescale) < 2 {
			continue
		}

		// 2. Converte as datas em Time (RFC3339)
		publishTime, err1 := time.Parse(time.RFC3339Nano, matchPublish[1])
		startTime, err2 := time.Parse(time.RFC3339Nano, matchStart[1])
		duration, err3 := strconv.ParseFloat(matchDuration[1], 64)
		timescale, err4 := strconv.ParseFloat(matchTimescale[1], 64)

		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			fmt.Println("❌ Erro ao converter tempos do MPD")
			continue
		}

		// 3. Calcula a duração exata do segmento em segundos
		segmentDurationSec := duration / timescale // Ex: 2000000 / 1000000 = 2.0s

		// 4. Calcula o número do segmento decorrido (começando em 1)
		elapsedSeconds := publishTime.Sub(startTime).Seconds()
		segmentoCalculado := int(math.Floor(elapsedSeconds/segmentDurationSec)) + 1

		if segmentoCalculado <= 0 {
			continue
		}

		Mutex.RLock()
		jaProcessado := segmentoCalculado <= UltimoSegmentoFisico
		Mutex.RUnlock()

		if jaProcessado {
			continue
		}

		// ==================================================
		// NOVO SEGMENTO DETECTADO
		// ==================================================

		Mutex.Lock()
		UltimoSegmentoFisico = segmentoCalculado
		SegmentoGlobalAtual = segmentoCalculado

		segmentoRelativo := 0
		if SegmentosPorLoop > 0 {
			segmentoRelativo = (segmentoCalculado - 1) % SegmentosPorLoop
		} else {
			segmentoRelativo = segmentoCalculado
		}
		Mutex.Unlock()

		fmt.Printf("📦 Novo segmento detectado: %d | 🔁 Relativo: %d\n", segmentoCalculado, segmentoRelativo)

		// ==================================================
		// GERA/PUBLICA EVENTOS MQTT
		// ==================================================

		GerarEventos(
			segmentoCalculado,
			segmentoRelativo,
		)
	}
}
