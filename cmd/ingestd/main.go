package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tharunn0/ingestd/internal/config"
	"github.com/tharunn0/ingestd/internal/dlq"
	"github.com/tharunn0/ingestd/internal/http/handlers"
	"github.com/tharunn0/ingestd/internal/http/server"
	"github.com/tharunn0/ingestd/internal/metrics"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/service"
	"github.com/tharunn0/ingestd/internal/worker"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.WarnLevel)

	if err := run(); err != nil {
		log.Error().Err(err).Msg("ingestd stopped with error")
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Init()

	deadLetters, err := dlq.New(cfg.DLQPath)
	if err != nil {
		return err
	}

	ingestionQueue := queue.New(cfg.QueueCapacity)
	pool := worker.New(ingestionQueue.Jobs(), cfg.WorkerCount, deadLetters)
	pool.Start()

	if err := metrics.Register(prometheus.DefaultRegisterer, ingestionQueue, pool, deadLetters); err != nil {
		return err
	}

	transformService := service.NewTransformService(ingestionQueue)
	telemetryHandler := handlers.NewTelemetryHandler(transformService)
	app := server.New(cfg, telemetryHandler)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	listenErr := make(chan error, 1)
	go func() {
		listenErr <- app.Listen(cfg.ListenAddr)
	}()

	var serveErr error
	select {
	case serveErr = <-listenErr:
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	return errors.Join(
		serveErr,
		app.ShutdownWithContext(shutdownCtx),
		pool.Shutdown(shutdownCtx),
		deadLetters.Close(),
	)
}
