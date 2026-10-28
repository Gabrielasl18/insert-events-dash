package main

import (
	"fmt"
	"log"
	"tcc-teste/config"
	"tcc-teste/services"
)

func main() {
	// 1. Conecta ao Broker MQTT
	err := config.InitMQTT("tcp://localhost:1883", "aop-receptor-go")
	if err != nil {
		log.Printf("⚠️ Aviso: Não foi possível conectar ao Broker MQTT: %v", err)
	} else {
		fmt.Println("✅ [AoP] Conectado ao Broker MQTT com sucesso!")
	}

	// 2. Define os endpoints do servidor Express (BCastService)
	serverBaseURL := "http://localhost:8081/live/dash"
	mpdURL := fmt.Sprintf("%s/playlist_1080p.mpd", serverBaseURL)

	fmt.Printf("📥 [AoP] Baixando manifesto estático do BCast: %s\n", mpdURL)

	// 3. Baixa e parseia o MPD
	err = services.FetchAndParseMPD(mpdURL)
	if err != nil {
		log.Fatalf("❌ Erro ao ler MPD da emissora: %v", err)
	}

	// 4. Inicia o monitoramento dos chunks via requisições HTTP ao servidor
	fmt.Println("🚀 [AoP] Iniciando monitoramento de segmentos via HTTP...")
	services.WatchSegmentsHTTP(serverBaseURL)
}
