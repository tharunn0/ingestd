package handlers

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/ingestd/internal/model"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/service"
)

type TelemetryHandler struct {
	serv *service.TransformService
}

func NewTelemetryHandler(serv *service.TransformService) *TelemetryHandler {
	return &TelemetryHandler{serv: serv}
}

func (h *TelemetryHandler) Ingest(c fiber.Ctx) error {
	var payload model.RawEvent
	if err := json.Unmarshal(c.Body(), &payload); err != nil || len(payload) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid telemetry payload",
		})
	}

	job := model.Job{
		ReceivedAt: time.Now(),
		Payload:    payload,
	}

	if err := h.serv.Enqueue(job); err != nil {
		if errors.Is(err, queue.ErrQueueFull) || errors.Is(err, queue.ErrQueueClosed) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
		return err
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"status": "accepted",
	})
}
