package services

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"tcc-teste/config"
	"tcc-teste/models"
)

var (
	Mutex *sync.RWMutex

	SegmentoGlobalAtual  *int
	UltimoSegmentoFisico *int
	SegmentosPorLoop     *int

	AppAtual string
	Events   []*models.Evento
)

// ==============================
// Estrutura mínima do MPD
// ==============================

type MPD struct {
	AvailabilityStartTime time.Time `xml:"availabilityStartTime,attr"`
	Periods               []Period  `xml:"Period"`
}

type Period struct {
	AdaptationSets []AdaptationSet `xml:"AdaptationSet"`
}

type AdaptationSet struct {
	Representations []Representation `xml:"Representation"`
}

type Representation struct {
	SegmentTemplate SegmentTemplate `xml:"SegmentTemplate"`
}

type SegmentTemplate struct {
	Timescale   int64 `xml:"timescale,attr"`
	Duration    int64 `xml:"duration,attr"`
	StartNumber int   `xml:"startNumber,attr"`
}

// ==============================
// Carrega events.json remoto
// ==============================

func ReadAndMarshalFile() error {
	fmt.Println("📥 Carregando eventos:", config.EventsJSON)

	resp, err := http.Get(config.EventsJSON)
	if err != nil {
		return fmt.Errorf(
			"erro buscando events.json: %w",
			err,
		)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"events.json retornou HTTP %d",
			resp.StatusCode,
		)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf(
			"erro lendo events.json: %w",
			err,
		)
	}

	var file models.EventsFile

	if err := json.Unmarshal(data, &file); err != nil {
		return fmt.Errorf(
			"erro fazendo parse do events.json: %w",
			err,
		)
	}

	if file.LastSeg == nil {
		return fmt.Errorf(
			"events.json não possui last_seg",
		)
	}

	segmentosPorLoop := *file.LastSeg + 1

	Mutex.Lock()

	SegmentosPorLoop = &segmentosPorLoop

	// Armazena o aplicativo lido do JSON (Ex: "CEFET/RJ TV")
	if file.App != nil {
		AppAtual = *file.App // Desreferencia o ponteiro para pegar o valor string
	} else {
		AppAtual = ""
	}

	Events = nil

	for _, item := range file.Actions {
		if item == nil || item.Action == nil {
			continue
		}

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

	Mutex.Unlock()

	fmt.Printf(
		"✅ App carregado: %s\n",
		AppAtual,
	)

	fmt.Printf(
		"✅ %d Events loaded\n",
		len(Events),
	)

	fmt.Printf(
		"✅ segmentosPorLoop = %d\n",
		segmentosPorLoop,
	)

	for _, ev := range Events {
		if ev == nil {
			continue
		}
	}

	return nil
}

// ==============================
// Monitora o MPD remoto
// ==============================

func WatchSegments() {
	if Mutex == nil {
		Mutex = &sync.RWMutex{}
	}

	fmt.Println("👀 Monitorando MPD realtime...")
	fmt.Println("📡 MPD:", config.DashMPD)

	ultimoProcessado := -1

	for {
		segmentoAtual, err := getCurrentSegment()

		if err != nil {
			fmt.Println(
				"❌ Erro lendo MPD:",
				err,
			)

			time.Sleep(1 * time.Second)
			continue
		}

		Mutex.Lock()

		SegmentoGlobalAtual = &segmentoAtual
		UltimoSegmentoFisico = &segmentoAtual

		segmentoRelativo := 0

		if SegmentosPorLoop != nil &&
			*SegmentosPorLoop > 0 {

			segmentoRelativo =
				(segmentoAtual - 1) %
					*SegmentosPorLoop

			if segmentoRelativo < 0 {
				segmentoRelativo +=
					*SegmentosPorLoop
			}
		}

		Mutex.Unlock()

		// Só gera eventos quando entrou em um novo segmento.
		if segmentoAtual != ultimoProcessado {
			ultimoProcessado = segmentoAtual

			fmt.Println()
			fmt.Println("📦 segmento global:", segmentoAtual)
			fmt.Println(
				"🔁 segmento relativo:",
				segmentoRelativo,
			)

			fmt.Println(
				"🎯 Verificando eventos para segmento:",
				segmentoRelativo,
			)

			GerarEventos(
				segmentoAtual,
				segmentoRelativo,
			)
		}

		time.Sleep(1 * time.Second)
	}
}

// ==============================
// Descobre segmento atual pelo MPD
// ==============================

func getCurrentSegment() (int, error) {
	resp, err := http.Get(config.DashMPD)

	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf(
			"MPD retornou HTTP %d",
			resp.StatusCode,
		)
	}

	data, err := io.ReadAll(resp.Body)

	if err != nil {
		return 0, err
	}

	var mpd MPD

	if err := xml.Unmarshal(data, &mpd); err != nil {
		return 0, fmt.Errorf(
			"erro parseando MPD: %w",
			err,
		)
	}

	if mpd.AvailabilityStartTime.IsZero() {
		return 0, fmt.Errorf(
			"MPD não possui availabilityStartTime",
		)
	}

	var template *SegmentTemplate

	for _, period := range mpd.Periods {
		for _, adaptation := range period.AdaptationSets {

			if len(adaptation.Representations) == 0 {
				continue
			}

			t := &adaptation.Representations[0].SegmentTemplate

			if t.Timescale > 0 &&
				t.Duration > 0 {

				template = t
				break
			}
		}

		if template != nil {
			break
		}
	}

	if template == nil {
		return 0, fmt.Errorf(
			"SegmentTemplate não encontrado no MPD",
		)
	}

	segmentDuration := time.Duration(
		float64(template.Duration) /
			float64(template.Timescale) *
			float64(time.Second),
	)

	if segmentDuration <= 0 {
		return 0, fmt.Errorf(
			"duração de segmento inválida",
		)
	}

	elapsed := time.Since(
		mpd.AvailabilityStartTime,
	)

	if elapsed < 0 {
		return template.StartNumber, nil
	}

	segmentOffset :=
		int(elapsed / segmentDuration)

	currentSegment :=
		template.StartNumber +
			segmentOffset

	return currentSegment, nil
}

// ==============================
// Helpers
// ==============================

func stringValue(v *string) string {
	if v == nil {
		return ""
	}

	return *v
}

func intValue(v *int) int {
	if v == nil {
		return 0
	}

	return *v
}
