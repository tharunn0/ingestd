package main

import (
	"log"

	"github.com/tharunn0/ingestd/internal/config"
	"github.com/tharunn0/ingestd/internal/http/handlers"
	"github.com/tharunn0/ingestd/internal/http/server"
	"github.com/tharunn0/ingestd/internal/service"
)

func main() {

	cfg := config.Init()

	transformService := service.NewTransformService()
	telemetryHandler := handlers.NewTelemetryHandler(transformService)

	app := server.New(cfg, telemetryHandler)

	log.Println("Starting ingestd on :3000...")
	if err := app.Listen(":3000"); err != nil {
		log.Fatalf("server stopped with error: %v", err)
	}
}
