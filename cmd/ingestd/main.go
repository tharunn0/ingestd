package main

import (
	"log"

	"github.com/tharunn0/ingestd/internal/config"
	"github.com/tharunn0/ingestd/internal/server"
)

func main() {

	cfg := config.Init()

	app := server.New(cfg)

	log.Println("Starting ingestd on :3000...")
	if err := app.Listen(":3000"); err != nil {
		log.Fatalf("server stopped with error: %v", err)
	}
}
