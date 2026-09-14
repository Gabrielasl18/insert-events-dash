package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"tcc-teste/config"
	"tcc-teste/services"
)

func main() {
	// ==============================
	// Inicialização
	// ==============================

	services.Mutex = &sync.RWMutex{}

	// MQTT
	if err := services.InitMQTT(); err != nil {
		panic(err)
	}

	// Eventos vêm do servidor 8081
	if err := services.ReadAndMarshalFile(); err != nil {
		panic(err)
	}

	// Monitor do MPD remoto
	go services.WatchSegments()

	// ==============================
	// Servidor HTTP
	// ==============================

	fs := http.FileServer(
		http.Dir(config.Directory),
	)

	http.Handle(
		"/",
		http.HandlerFunc(
			func(
				w http.ResponseWriter,
				r *http.Request,
			) {
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

				// MPD é obtido do servidor 8081
				if strings.HasSuffix(
					r.URL.Path,
					".mpd",
				) {
					services.HandleMPD(
						w,
						r,
					)

					return
				}

				// Demais arquivos continuam
				// sendo servidos normalmente.
				fs.ServeHTTP(
					w,
					r,
				)
			},
		),
	)

	fmt.Println(
		"🚀 Servidor rodando em",
		config.Porta,
	)

	fmt.Println(
		"📡 MPD:",
		config.DashMPD,
	)

	fmt.Println(
		"📋 Eventos:",
		config.EventsJSON,
	)

	fmt.Println(
		"📤 MQTT:",
		config.MQTTTopic,
	)

	if err := http.ListenAndServe(
		config.Porta,
		nil,
	); err != nil {
		panic(err)
	}
}
