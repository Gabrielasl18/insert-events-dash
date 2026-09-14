package services

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"tcc-teste/config"
	"tcc-teste/models"

	"github.com/fsnotify/fsnotify"
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

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}

	defer watcher.Close()

	if err := watcher.Add(config.Directory); err != nil {
		panic(err)
	}

	// Detecta:
	//
	// chunk-stream0-00001.m4s
	// chunk-stream0-00002.m4s
	// chunk-stream0-00003.m4s
	//
	// Ignora:
	//
	// chunk-stream1-xxxxx.m4s
	// *.tmp

	re := regexp.MustCompile(
		`^chunk-stream0-0*(\d+)\.m4s$`,
	)

	fmt.Println(
		"👀 Monitorando segmentos realtime...",
	)

	for {

		select {

		case event, ok := <-watcher.Events:

			if !ok {
				return
			}

			if event.Op&fsnotify.Create != fsnotify.Create &&
				event.Op&fsnotify.Rename != fsnotify.Rename {
				continue
			}

			name := filepath.Base(event.Name)

			fmt.Println("📄 arquivo:", name)

			// Ignora temporários.
			if strings.HasSuffix(name, ".tmp") {
				continue
			}

			match := re.FindStringSubmatch(name)

			fmt.Println("🔎 match:", match)

			if len(match) < 2 {
				continue
			}

			segmentoFisico, err := strconv.Atoi(match[1])

			if err != nil {
				fmt.Println(
					"❌ Erro convertendo segmento:",
					err,
				)
				continue
			}

			// ==================================================
			// ATUALIZA SEGMENTO
			// ==================================================

			Mutex.Lock()

			UltimoSegmentoFisico = segmentoFisico
			SegmentoGlobalAtual = segmentoFisico

			segmentoRelativo := 0

			if SegmentosPorLoop > 0 {

				segmentoRelativo =
					(segmentoFisico - 1) %
						SegmentosPorLoop
			}

			Mutex.Unlock()

			fmt.Println(
				"📦 segmento físico:",
				segmentoFisico,
			)

			fmt.Println(
				"🔁 segmento relativo:",
				segmentoRelativo,
			)

			// ==================================================
			// GERA/PUBLICA EVENTOS
			// ==================================================

			fmt.Println(
				"🎯 Verificando eventos para segmento:",
				segmentoRelativo,
			)

			GerarEventos(
				segmentoFisico,
				segmentoRelativo,
			)

		case err, ok := <-watcher.Errors:

			if !ok {
				return
			}

			fmt.Println(
				"❌ watch error:",
				err,
			)
		}
	}
}
