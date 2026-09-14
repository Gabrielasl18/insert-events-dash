package services

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"tcc-teste/config"
	"tcc-teste/models"
	"time"
)

// HandleMPD busca o MPD diretamente no BcastService,
// injeta os eventos correspondentes ao segmento atual
// e devolve o MPD para quem fez a requisição.
func HandleMPD(w http.ResponseWriter, r *http.Request) {

	fmt.Println("🔥 MPD REQUEST:", r.URL.Path)

	// =========================================================
	// BUSCA O MPD NO BCAST
	// =========================================================

	resp, err := http.Get(config.DashURL)

	if err != nil {
		fmt.Println("❌ Erro buscando MPD:", err)

		http.Error(
			w,
			"Erro ao buscar MPD",
			http.StatusBadGateway,
		)

		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {

		fmt.Println(
			"❌ Bcast respondeu:",
			resp.StatusCode,
		)

		http.Error(
			w,
			"MPD não encontrado",
			resp.StatusCode,
		)

		return
	}

	// =========================================================
	// LÊ O MPD
	// =========================================================

	data, err := io.ReadAll(resp.Body)

	if err != nil {

		fmt.Println(
			"❌ Erro lendo MPD:",
			err,
		)

		http.Error(
			w,
			"Erro lendo MPD",
			http.StatusInternalServerError,
		)

		return
	}

	mpd := string(data)

	// =========================================================
	// OBTÉM O SEGMENTO ATUAL
	// =========================================================

	Mutex.RLock()

	segmentoGlobal := SegmentoGlobalAtual

	Mutex.RUnlock()

	if segmentoGlobal == 0 {
		segmentoGlobal = 1
	}

	if SegmentosPorLoop <= 0 {

		fmt.Println(
			"⚠️ SegmentosPorLoop inválido:",
			SegmentosPorLoop,
		)

		http.Error(
			w,
			"SegmentosPorLoop inválido",
			http.StatusInternalServerError,
		)

		return
	}

	segmentoRelativo :=
		(segmentoGlobal - 1) % SegmentosPorLoop

	fmt.Println(
		"➡️ MPD REQUEST | global:",
		segmentoGlobal,
		"| relativo:",
		segmentoRelativo,
	)

	// =========================================================
	// GERA EVENTOS
	// =========================================================

	eventsXML := GerarEventos(
		segmentoGlobal,
		segmentoRelativo,
	)

	if eventsXML != "" {

		if strings.Contains(mpd, "</Period>") {

			mpd = strings.Replace(
				mpd,
				"</Period>",
				"\n"+eventsXML+"\n</Period>",
				1,
			)

			fmt.Println(
				"⚡ [SUCESSO] EventStream injetado no MPD!",
			)

		} else {

			fmt.Println(
				"❌ MPD não possui </Period>",
			)
		}
	}

	// =========================================================
	// ATUALIZA PUBLISH TIME
	// =========================================================

	now := time.Now().
		UTC().
		Format(time.RFC3339Nano)

	rePublish := regexp.MustCompile(
		`publishTime="[^"]+"`,
	)

	mpd = rePublish.ReplaceAllString(
		mpd,
		`publishTime="`+now+`"`,
	)

	// =========================================================
	// ATUALIZA MINIMUM UPDATE PERIOD
	// =========================================================

	reMUP := regexp.MustCompile(
		`minimumUpdatePeriod="[^"]+"`,
	)

	mpd = reMUP.ReplaceAllString(
		mpd,
		`minimumUpdatePeriod="PT1S"`,
	)

	// =========================================================
	// DEVOLVE O MPD
	// =========================================================

	w.Header().Set(
		"Content-Type",
		"application/dash+xml",
	)

	w.Header().Set(
		"Access-Control-Allow-Origin",
		"*",
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

	_, _ = w.Write([]byte(mpd))
}

// =============================================================
// GERA EVENTOS MPEG-DASH
// =============================================================

func GerarEventos(
	segmentoGlobal int,
	segmentoRelativo int,
) string {

	stream := models.EventStream{
		XMLName: xml.Name{
			Local: "EventStream",
		},

		SchemeID: config.EventScheme,

		Value: config.EventValue,

		Timescale: 1000000,
	}

	for _, ev := range Events {

		fmt.Printf(
			"🔎 EVENT CHECK | global=%d | relativo=%d | start=%d | end=%d | name=%s\n",
			segmentoGlobal,
			segmentoRelativo,
			ev.SegStart,
			ev.SegEnd,
			ev.Name,
		)

		// =====================================================
		// START EVENT
		// =====================================================

		if segmentoRelativo == ev.SegStart {

			payload, err := json.Marshal(
				map[string]any{
					"type":        "start",
					"name":        ev.Name,
					"description": ev.Desc,
					"color":       ev.Color,
				},
			)

			if err != nil {

				fmt.Println(
					"❌ Erro criando payload START:",
					err,
				)

				continue
			}

			fmt.Printf(
				"🚀 START EVENT | %s | SegStart=%d | Global=%d | Relativo=%d\n",
				ev.Name,
				ev.SegStart,
				segmentoGlobal,
				segmentoRelativo,
			)

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
						"%s_start_loop_%d",
						ev.Name,
						segmentoGlobal,
					),

					Data: string(payload),
				},
			)
		}

		// =====================================================
		// END EVENT
		// =====================================================

		if segmentoRelativo == ev.SegEnd {

			payload, err := json.Marshal(
				map[string]any{
					"type":        "end",
					"name":        ev.Name,
					"description": ev.Desc,
					"color":       ev.Color,
				},
			)

			if err != nil {

				fmt.Println(
					"❌ Erro criando payload END:",
					err,
				)

				continue
			}

			fmt.Printf(
				"🛑 END EVENT | %s | SegEnd=%d | Global=%d | Relativo=%d\n",
				ev.Name,
				ev.SegEnd,
				segmentoGlobal,
				segmentoRelativo,
			)

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
						"%s_end_loop_%d",
						ev.Name,
						segmentoGlobal,
					),

					Data: string(payload),
				},
			)
		}
	}

	// =========================================================
	// NENHUM EVENTO
	// =========================================================

	if len(stream.EventItems) == 0 {
		return ""
	}

	// =========================================================
	// SERIALIZA XML
	// =========================================================

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

	return string(output)
}
