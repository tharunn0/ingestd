package middlewares_test

import (
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/ingestd/internal/http/middlewares"
)

func setupTestApp(secret string) *fiber.App {
	app := fiber.New()
	app.Get("/protected", middlewares.AuthMiddleware(secret), func(c fiber.Ctx) error {
		return c.SendString("ok")
	})
	return app
}

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	app := setupTestApp("my-secret")
	req, _ := http.NewRequest(http.MethodGet, "/protected", nil)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_ValidXAPIKey(t *testing.T) {
	app := setupTestApp("my-secret")
	req, _ := http.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("X-API-Key", "my-secret")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_ValidBearer(t *testing.T) {
	app := setupTestApp("my-secret")
	req, _ := http.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer my-secret")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_InvalidKey(t *testing.T) {
	app := setupTestApp("my-secret")
	req, _ := http.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("X-API-Key", "wrong-secret")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}
