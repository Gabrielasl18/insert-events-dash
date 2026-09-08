package main

import (
	"fmt"
	"net/http"
	"strings"
	"tcc-teste/config"
	"tcc-teste/services"
)

func main() {
	// ORDEM 1: Carrega os metadados do JSON de eventos para a memória global
	if err := services.ReadAndMarshalFile("./events.json"); err != nil {
		panic(err)
	}

	// ORDEM 2: Inicia o watcher em background para monitorar a gravação física de m4s
	go services.WatchSegments()

	// ORDEM 3: Configura o servidor HTTP de arquivos de mídia estáticos
	fs := http.FileServer(http.Dir(config.Directory))

	http.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")

		// Se a requisição pedir o manifesto, envia para tratamento dinâmico
		if strings.HasSuffix(r.URL.Path, ".mpd") {
			services.HandleMPD(w, r)
			return
		}

		fs.ServeHTTP(w, r)
	}))

	fmt.Printf("🚀 Servidor DASH rodando em http://localhost%s\n", config.Porta)

	// ORDEM 4: Abre o canal de escuta na porta TCP configurada
	err := http.ListenAndServe(config.Porta, nil)
	if err != nil {
		panic(err)
	}
}
