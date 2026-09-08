package services

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"tcc-teste/config"
	"tcc-teste/models"
	"time"
)

func HandleMPD(w http.ResponseWriter, r *http.Request) {
	fmt.Println("🔥 MPD REQUEST:", r.URL.Path)

	path := config.Directory + r.URL.Path

	data, err := os.ReadFile(path)
	if err != nil {
		http.Error(w, "MPD não encontrado", 404)
		return
	}

	mpd := string(data)

	Mutex.RLock()
	segmentoGlobal := SegmentoGlobalAtual
	Mutex.RUnlock()

	if segmentoGlobal == 0 {
		segmentoGlobal = 1
	}

	segmentoRelativo := (segmentoGlobal - 1) % SegmentosPorLoop

	fmt.Println("➡️ MPD REQUEST | global:", segmentoGlobal, "| relativo:", segmentoRelativo)

	eventsXML := GerarEventos(segmentoGlobal, segmentoRelativo)

	if eventsXML != "" {
		mpd = strings.Replace(
			mpd,
			"</Period>",
			"\n"+eventsXML+"\n</Period>",
			1,
		)

		path_modify := config.Directory + "/modify" + r.URL.Path

		err := os.WriteFile(path_modify, []byte(mpd), 0644)
		if err != nil {
			fmt.Println("erro salvando MPD:", err)
		}
		fmt.Println("⚡ [SUCESSO] EventStream injetado no MPD enviado!")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	rePublish := regexp.MustCompile(`publishTime="[^"]+"`)
	mpd = rePublish.ReplaceAllString(mpd, `publishTime="`+now+`"`)

	reMUP := regexp.MustCompile(`minimumUpdatePeriod="[^"]+"`)
	mpd = reMUP.ReplaceAllString(mpd, `minimumUpdatePeriod="PT1S"`)

	fmt.Println("========== MPD ENVIADO ==========")
	fmt.Println(mpd)
	fmt.Println("=================================")

	w.Header().Set("Content-Type", "application/dash+xml")
	_, _ = w.Write([]byte(mpd))
}

func GerarEventos(segmentoGlobal int, segmentoRelativo int) string {
	stream := models.EventStream{
		XMLName:   xml.Name{Local: "EventStream"},
		SchemeID:  config.EventScheme,
		Value:     config.EventValue,
		Timescale: 1000000,
	}

	for _, ev := range Events {
		if segmentoRelativo == ev.SegStart {
			payload, _ := json.Marshal(map[string]any{
				"type":        "start",
				"name":        ev.Name,
				"description": ev.Desc,
				"color":       ev.Color,
			})

			fmt.Printf(
				"Evento=%s | SegStart=%d | SegEnd=%d | Relativo=%d\n",
				ev.Name,
				ev.SegStart,
				ev.SegEnd,
				segmentoRelativo,
			)

			fmt.Println(
				"global:", segmentoGlobal,
				"relativo:", segmentoRelativo,
				"eventos:", len(stream.EventItems),
			)

			stream.EventItems = append(
				stream.EventItems,
				models.Event{
					XMLName: xml.Name{Local: "Event"},
					PresentationTime: int64(segmentoGlobal-1) *
						config.SegmentDurationUs,
					Duration: config.SegmentDurationUs,
					ID:       fmt.Sprintf("%s_start_loop", ev.Name),
					Data:     string(payload),
				},
			)
		}

		if segmentoRelativo == ev.SegEnd {
			payload, _ := json.Marshal(map[string]any{
				"type":        "end",
				"name":        ev.Name,
				"description": ev.Desc,
				"color":       ev.Color,
			})

			stream.EventItems = append(stream.EventItems, models.Event{
				XMLName: xml.Name{Local: "Event"},
				PresentationTime: int64(segmentoGlobal-1) *
					config.SegmentDurationUs,
				Duration: config.SegmentDurationUs,
				ID:       fmt.Sprintf("%s_end_loop", ev.Name),
				Data:     string(payload),
			})
		}
	}

	if len(stream.EventItems) == 0 {
		return ""
	}

	output, err := xml.MarshalIndent(stream, "    ", "    ")
	if err != nil {
		return ""
	}

	return string(output)
}
