package handlers

import (
	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/ingestd/internal/service"
)

type TelemetryHandler struct {
	serv *service.TransformService
}

func NewTelemetryHandler(serv *service.TransformService) *TelemetryHandler {
	return &TelemetryHandler{serv: serv}
}

func (h *TelemetryHandler) Ingest(c fiber.Ctx) error {
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"status": "accepted",
	})
}
