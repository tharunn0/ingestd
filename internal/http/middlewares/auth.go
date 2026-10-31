package middlewares

import (
	"strings"

	"github.com/gofiber/fiber/v3"
)

// AuthMiddleware validates either the X-API-Key header or Authorization: Bearer <key>.
func AuthMiddleware(secretKey string) fiber.Handler {
	return func(c fiber.Ctx) error {
		apiKey := c.Get("X-API-Key")
		if apiKey == "" {
			authHeader := c.Get("Authorization")
			if authHeader != "" {
				parts := strings.SplitN(authHeader, " ", 2)
				if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
					apiKey = parts[1]
				}
			}
		}

		if apiKey == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "missing authentication credentials",
			})
		}

		if apiKey != secretKey {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "invalid API key",
			})
		}

		return c.Next()
	}
}
