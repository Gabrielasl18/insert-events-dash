package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"tcc-teste/config"
	"tcc-teste/models"
	"time"
)

var (
	Mutex = &sync.RWMutex{}

	SegmentoGlobalAtual  int
	UltimoSegmentoFisico int
	SegmentosPorLoop     int

	Events []*models.Evento
)

// =============================================================
// CARREGA EVENTS.JSON
// =============================================================

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

	return nil
}

// =============================================================
// MONITORAMENTO DOS SEGMENTOS DASH
// =============================================================

func WatchSegments() {

	fmt.Println(
		"👀 Monitorando chunks DASH...",
	)

	fmt.Println(
		"📡 http://localhost:8081/live/dash/",
	)

	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	ultimoDetectado := 0

	for {

		// =====================================================
		// PROCURA O PRIMEIRO SEGMENTO
		// =====================================================

		if ultimoDetectado == 0 {

			for segmento := 1; segmento <= 10000; segmento++ {

				url := fmt.Sprintf(
					"http://localhost:8081/live/dash/chunk-stream0-%05d.m4s",
					segmento,
				)

				resp, err := client.Head(url)

				if err != nil {
					break
				}

				resp.Body.Close()

				if resp.StatusCode == http.StatusOK {

					ultimoDetectado = segmento

					Mutex.Lock()

					UltimoSegmentoFisico =
						segmento

					SegmentoGlobalAtual =
						segmento

					Mutex.Unlock()

					fmt.Println(
						"📦 primeiro segmento encontrado:",
						segmento,
					)

					// Processa o primeiro segmento
					// caso ele contenha evento.
					AtualizarMPDComEventos(
						segmento,
					)

					break
				}
			}
		}

		// =====================================================
		// AINDA NÃO ENCONTROU SEGMENTO
		// =====================================================

		if ultimoDetectado == 0 {

			time.Sleep(
				1 * time.Second,
			)

			continue
		}

		// =====================================================
		// MONITORA PRÓXIMOS SEGMENTOS
		// =====================================================

		for {

			proximo := ultimoDetectado + 1

			url := fmt.Sprintf(
				"http://localhost:8081/live/dash/chunk-stream0-%05d.m4s",
				proximo,
			)

			resp, err := client.Head(url)

			if err != nil {
				break
			}

			resp.Body.Close()

			// Ainda não chegou.
			if resp.StatusCode != http.StatusOK {
				break
			}

			// =================================================
			// NOVO SEGMENTO ENCONTRADO
			// =================================================

			ultimoDetectado = proximo

			Mutex.Lock()

			UltimoSegmentoFisico =
				ultimoDetectado

			SegmentoGlobalAtual =
				ultimoDetectado

			segmentoGlobal :=
				SegmentoGlobalAtual

			var segmentoRelativo int

			if SegmentosPorLoop > 0 {

				segmentoRelativo =
					(ultimoDetectado - 1) %
						SegmentosPorLoop
			}

			fmt.Println(
				"📦 segmento físico:",
				ultimoDetectado,
			)

			fmt.Println(
				"🌐 segmento global:",
				segmentoGlobal,
			)

			fmt.Println(
				"🔁 segmento relativo:",
				segmentoRelativo,
			)

			Mutex.Unlock()

			// =================================================
			// ATUALIZA MPD SE HOUVER EVENTO
			// =================================================

			AtualizarMPDComEventos(
				segmentoGlobal,
			)
		}

		time.Sleep(
			500 * time.Millisecond,
		)
	}
}

// =============================================================
// BUSCA MPD, INJETA EVENTOS E ENVIA VIA PUT
// =============================================================

func AtualizarMPDComEventos(
	segmentoGlobal int,
) {

	if SegmentosPorLoop <= 0 {

		fmt.Println(
			"⚠️ SegmentosPorLoop inválido",
		)

		return
	}

	segmentoRelativo :=
		(segmentoGlobal - 1) %
			SegmentosPorLoop

	fmt.Printf(
		"📝 Verificando eventos | global=%d | relativo=%d\n",
		segmentoGlobal,
		segmentoRelativo,
	)

	// =========================================================
	// GERA EVENTOS PRIMEIRO
	// =========================================================

	eventsXML := GerarEventos(
		segmentoGlobal,
		segmentoRelativo,
	)

	// Se não existe evento nesse segmento,
	// não precisa buscar nem alterar o MPD.
	if eventsXML == "" {

		fmt.Println(
			"ℹ️ Nenhum evento para o segmento",
			segmentoGlobal,
		)

		return
	}

	// =========================================================
	// BUSCA MPD ATUAL NO BCAST
	// =========================================================

	resp, err := http.Get(
		config.DashURL,
	)

	if err != nil {

		fmt.Println(
			"❌ Erro buscando MPD:",
			err,
		)

		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {

		fmt.Println(
			"❌ Bcast respondeu:",
			resp.StatusCode,
		)

		return
	}

	data, err := io.ReadAll(
		resp.Body,
	)

	if err != nil {

		fmt.Println(
			"❌ Erro lendo MPD:",
			err,
		)

		return
	}

	mpd := string(data)

	// =========================================================
	// REMOVE EVENTSTREAMS GERADOS ANTERIORMENTE
	// =========================================================

	mpd = removerEventStreamsGerados(mpd)

	// =========================================================
	// INJETA NOVO EVENTSTREAM
	// =========================================================

	if !strings.Contains(
		mpd,
		"</Period>",
	) {

		fmt.Println(
			"❌ MPD não possui </Period>",
		)

		return
	}

	mpd = strings.Replace(
		mpd,
		"</Period>",
		"\n"+eventsXML+"\n</Period>",
		1,
	)

	fmt.Println(
		"⚡ [SUCESSO] EventStream injetado no MPD!",
	)

	// =========================================================
	// ENVIA MPD PARA O BCAST
	// =========================================================

	if err := AtualizarMPDNoBcast(mpd); err != nil {

		fmt.Println(
			"❌ Erro enviando MPD alterado:",
			err,
		)

		return
	}

	fmt.Println(
		"💾 MPD alterado enviado para o BcastService",
	)
}

// =============================================================
// REMOVE EVENTSTREAMS GERADOS PELO NOSSO SERVIÇO
// =============================================================

func removerEventStreamsGerados(
	mpd string,
) string {

	startTag := `<EventStream schemeIdUri="` +
		config.EventScheme

	start := 0

	for {

		index := strings.Index(
			mpd[start:],
			startTag,
		)

		if index == -1 {
			break
		}

		index += start

		endRelative := strings.Index(
			mpd[index:],
			"</EventStream>",
		)

		if endRelative == -1 {
			break
		}

		end := index +
			endRelative +
			len("</EventStream>")

		mpd =
			mpd[:index] +
				mpd[end:]

		start = index
	}

	return mpd
}

// =============================================================
// ENVIA MPD ALTERADO PARA O BCAST VIA PUT
// =============================================================

func AtualizarMPDNoBcast(
	mpd string,
) error {

	req, err := http.NewRequest(
		http.MethodPut,
		config.DashURL,
		strings.NewReader(mpd),
	)

	if err != nil {

		return fmt.Errorf(
			"erro criando PUT: %w",
			err,
		)
	}

	req.Header.Set(
		"Content-Type",
		"application/dash+xml",
	)

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Do(req)

	if err != nil {

		return fmt.Errorf(
			"erro enviando MPD para Bcast: %w",
			err,
		)
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 ||
		resp.StatusCode >= 300 {

		body, _ := io.ReadAll(
			resp.Body,
		)

		return fmt.Errorf(
			"Bcast respondeu %d: %s",
			resp.StatusCode,
			string(body),
		)
	}

	fmt.Println(
		"✅ MPD atualizado no BcastService",
	)

	return nil
}
