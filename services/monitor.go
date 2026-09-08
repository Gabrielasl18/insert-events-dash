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
	Mutex sync.RWMutex

	SegmentoGlobalAtual  int
	UltimoSegmentoFisico int

	SegmentosPorLoop int
	Events           []models.Evento
)

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
		Events = append(Events, models.Evento{
			SegStart: item.SegStart,
			SegEnd:   item.SegEnd,
			Name:     item.Action.Name,
			Desc:     item.Action.Description,
			Color:    item.Action.Color,
		})
	}

	fmt.Printf("✅ %d Events loaded\n", len(Events))
	fmt.Printf("✅ segmentosPorLoop = %d\n", SegmentosPorLoop)
	return nil
}

func WatchSegments() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	defer watcher.Close()

	// Listen to configutation file (file dash with chunks)
	if err := watcher.Add(config.Directory); err != nil {
		panic(err)
	}

	re := regexp.MustCompile(`chunk-stream0-0*(\d+)\.m4s$`)

	fmt.Println("👀 Monitorando segmentos realtime...")

	for {
		select {
		case event := <-watcher.Events:
			if event.Op&fsnotify.Create != fsnotify.Create &&
				event.Op&fsnotify.Rename != fsnotify.Rename {
				continue
			}

			name := filepath.Base(event.Name)

			fmt.Println(name)

			if strings.HasSuffix(name, ".tmp") {
				continue
			}

			// até a linha 93 verifica se os chunks (ex: chunk-stream-0-0223.m4s) estão vindo
			match := re.FindStringSubmatch(name)
			fmt.Println(match)

			if len(match) < 2 {
				continue
			}

			segmentoFisico, err := strconv.Atoi(match[1])
			if err != nil {
				continue
			}

			Mutex.Lock()

			UltimoSegmentoFisico = segmentoFisico
			SegmentoGlobalAtual = segmentoFisico

			segmentoRelativo :=
				(segmentoFisico - 1) % SegmentosPorLoop

			fmt.Println("📦 segmento físico:", segmentoFisico)
			fmt.Println("🔁 segmento relativo:", segmentoRelativo)

			Mutex.Unlock()

		case err := <-watcher.Errors:
			fmt.Println("watch error:", err)
		}
	}
}
