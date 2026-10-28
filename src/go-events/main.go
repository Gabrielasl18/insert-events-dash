package main

import (
	"context"
	"fmt"
	"go-events/config"
	"go-events/services"
	"log"
	"net/url"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	mu            sync.Mutex
	cancelWatcher context.CancelFunc
	currentURL    string
	watcherID     uint64
)

func main() {
	brokerURL := "tcp://localhost:1883"
	clientID := "aop-receiver-go"

	// Callback executado na conexão inicial e em cada reconexão automática
	onConnectHandler := func(c mqtt.Client) {
		fmt.Println("[AoP] Conectado ao Broker MQTT")
		fmt.Println("[AoP] Aguardando URLs de vídeo")

		topics := []string{
			"aop/display/layers/video/url",
			"video/url",
		}

		for _, topic := range topics {
			token := c.Subscribe(topic, 1, handleVideoURL)
			token.Wait()
			if err := token.Error(); err != nil {
				log.Printf("❌ [Error] Subscribe %s: %v", topic, err)
			}
		}
	}

	// Inicializa o MQTT utilizando a função centralizada do config
	if err := config.InitMQTT(brokerURL, clientID, onConnectHandler); err != nil {
		log.Fatalf("❌ [Error] Conexão MQTT: %v", err)
	}

	// Mantém a aplicação rodando
	select {}
}

func handleVideoURL(client mqtt.Client, msg mqtt.Message) {
	raw := strings.TrimSpace(string(msg.Payload()))

	mu.Lock()
	defer mu.Unlock()

	// 1. Se a URL recebida for vazia (ex: usuário saiu da tela / parou o vídeo),
	// cancela o watcher atual imediatamente e limpa o estado.
	if raw == "" {
		if cancelWatcher != nil {
			cancelWatcher()
			cancelWatcher = nil
		}
		currentURL = ""
		return
	}

	newURL := normalizeURL(raw)

	// Se for a mesma URL já em execução, ignora
	if newURL == currentURL {
		return
	}

	// 2. Se mudou de URL, encerra o monitor que estava rodando antes
	if cancelWatcher != nil {
		cancelWatcher()
		cancelWatcher = nil
	}

	currentURL = newURL
	watcherID++

	parsed, err := url.Parse(newURL)
	if err != nil || !strings.HasSuffix(strings.ToLower(parsed.Path), ".mpd") {
		return
	}

	// 3. Cria um novo contexto de cancelamento para o novo monitor
	ctx, cancel := context.WithCancel(context.Background())
	cancelWatcher = cancel
	id := watcherID

	go func() {
		services.WatchSegmentsMPD(ctx, newURL)

		mu.Lock()
		if watcherID == id {
			cancelWatcher = nil
		}
		mu.Unlock()
	}()
}

func normalizeURL(raw string) string {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if file, found := strings.CutPrefix(raw, "/videoStreamProxy/"); found {
		return "http://localhost:8081/live/dash/" + file
	}
	return "http://localhost:8081/live/" + strings.TrimLeft(raw, "/")
}
