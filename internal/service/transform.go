package service

import (
	"github.com/tharunn0/ingestd/internal/model"
	"github.com/tharunn0/ingestd/internal/queue"
)

type TransformService struct {
	queue *queue.IngestionQueue
}

func NewTransformService(q *queue.IngestionQueue) *TransformService {
	return &TransformService{queue: q}
}

func (s *TransformService) Enqueue(job model.Job) error {
	return s.queue.Enqueue(job)
}
