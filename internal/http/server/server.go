package server

import (
	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/ingestd/internal/config"
	"github.com/tharunn0/ingestd/internal/http/handlers"
	"github.com/tharunn0/ingestd/internal/http/middlewares"
)

// New initializes and configures a new Fiber app instance.
func New(cfg config.Config, telemetry *handlers.TelemetryHandler) *fiber.App {
	app := fiber.New()

	app.Get("/health", func(c fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status": "ok",
		})
	})

	v1 := app.Group("/v1", middlewares.AuthMiddleware(string(cfg.AuthSecretKey)))
	v1.Post("/telemetry", telemetry.Ingest)

	return app
}
