package config

import (
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	AuthSecretKey []byte
}

func Init() Config {
	_ = godotenv.Load()

	authSecretKey := os.Getenv("INGESTD_AUTH_SECRET_KEY")
	if len(authSecretKey) == 0 {
		authSecretKey = "default-dev-secret-key"
	}

	return Config{
		AuthSecretKey: []byte(authSecretKey),
	}
}
