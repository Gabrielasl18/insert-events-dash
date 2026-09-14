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

	/*
		===========================================================
		ORDEM 0: Inicializa estado compartilhado
		===========================================================
	*/

	services.Mutex = &sync.RWMutex{}

	/*
		===========================================================
		ORDEM 1: Conecta no MQTT
		===========================================================
	*/

	if err := services.InitMQTT(); err != nil {
		panic(err)
	}

	/*
		===========================================================
		ORDEM 2: Carrega os metadados do JSON de eventos
		===========================================================
	*/

	if err := services.ReadAndMarshalFile("./events.json"); err != nil {
		panic(err)
	}

	/*
		===========================================================
		ORDEM 3: Monitora os segmentos físicos
		===========================================================
	*/

	go services.WatchSegments()

	/*
		===========================================================
		ORDEM 4: Servidor HTTP
		===========================================================
	*/

	fs := http.FileServer(
		http.Dir(config.Directory),
	)

	http.Handle(
		"/",
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {

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

				/*
					Se a requisição pedir o manifesto,
					envia para tratamento dinâmico.
				*/

				if strings.HasSuffix(
					r.URL.Path,
					".mpd",
				) {

					services.HandleMPD(w, r)

					return
				}

				fs.ServeHTTP(w, r)
			},
		),
	)

	fmt.Printf(
		"🚀 Servidor DASH rodando em http://localhost%s\n",
		config.Porta,
	)

	/*
		===========================================================
		ORDEM 5: Abre servidor
		===========================================================
	*/

	err := http.ListenAndServe(
		config.Porta,
		nil,
	)

	if err != nil {
		panic(err)
	}
}
