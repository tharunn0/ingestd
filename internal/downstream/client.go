package downstream

import (
	"context"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/client"
	"github.com/tharunn0/ingestd/internal/model"
)

type Client struct {
	url  string
	http *client.Client
}

func New(url string, timeout time.Duration) *Client {
	return &Client{
		url:  url,
		http: client.New().SetTimeout(timeout),
	}
}

func (c *Client) Send(ctx context.Context, event *model.TransformedEvent) error {
	resp, err := c.http.R().SetContext(ctx).SetJSON(event).Post(c.url)
	if err != nil {
		return fmt.Errorf("downstream request failed: %w", err)
	}
	defer resp.Close()

	if status := resp.StatusCode(); status < fiber.StatusOK || status >= fiber.StatusMultipleChoices {
		return fmt.Errorf("downstream responded with status %d", status)
	}
	return nil
}
