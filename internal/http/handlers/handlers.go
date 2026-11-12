package handlers

import (
	"errors"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tharunn0/ingestd/internal/metrics"
	"github.com/tharunn0/ingestd/internal/model"
	"github.com/tharunn0/ingestd/internal/queue"
	"github.com/tharunn0/ingestd/internal/service"
	"github.com/valyala/fastjson"
)

const rejectionLogsPerSecond = 5

type TelemetryHandler struct {
	serv *service.TransformService

	rejections zerolog.Logger
	pool       fastjson.ParserPool
}

func NewTelemetryHandler(serv *service.TransformService) *TelemetryHandler {
	return &TelemetryHandler{
		serv: serv,
		rejections: log.Sample(&zerolog.BurstSampler{
			Burst:  rejectionLogsPerSecond,
			Period: time.Second,
		}),
	}
}

func (h *TelemetryHandler) Ingest(c fiber.Ctx) error {
	p := h.pool.Get()
	defer h.pool.Put(p)

	v, err := p.ParseBytes(c.Body())
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid telemetry payload",
		})
	}

	obj, err := v.Object()
	if err != nil || obj.Len() == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid telemetry payload",
		})
	}

	payload := make(model.RawEvent, obj.Len())
	var hasInvalidValue bool
	obj.Visit(func(k []byte, v *fastjson.Value) {
		if hasInvalidValue {
			return
		}
		valBytes, err := v.StringBytes()
		if err != nil {
			hasInvalidValue = true
			return
		}
		payload[string(k)] = string(valBytes)
	})

	if hasInvalidValue {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid telemetry payload",
		})
	}

	job := model.Job{
		TenantID:   payload["tenant_id"],
		ReceivedAt: time.Now(),
		Payload:    payload,
	}

	if err := h.serv.Enqueue(job); err != nil {
		if errors.Is(err, queue.ErrQueueFull) || errors.Is(err, queue.ErrQueueClosed) {
			metrics.EventsRejected.Inc()
			h.rejections.Warn().Err(err).Msg("telemetry rejected")
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
		return err
	}

	metrics.EventsReceived.Inc()
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"status": "accepted",
	})
}
