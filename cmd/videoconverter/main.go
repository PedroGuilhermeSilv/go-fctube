package main

import (
	"database/sql"
	"fmt"
	"imersaofc/internal/converter"
	"log/slog"
	"os"

	_ "github.com/lib/pq"
)


func connectPostgres() (*sql.DB,error) {
	user := getEnvOrDefault("POSTGRES_USER", "user")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	host := getEnvOrDefault("POSTGRES_HOST", "postgres")
	port := getEnvOrDefault("POSTGRES_PORT", "5432")
	dbname := getEnvOrDefault("POSTGRES_DB", "converter")
	sslmode := getEnvOrDefault("POSTGRES_SSL_MODE", "disable")

	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=%s", user, password, host, port, dbname, sslmode)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		slog.Error("failed to connect to postgres", slog.String("conn_str", connStr))
		return nil, err

	}

	err = db.Ping()
	if err != nil {
		slog.Error("failed to ping postgres", slog.String("conn_str", connStr))
		return nil, err
	}

	slog.Info("connected to postgres", slog.String("conn_str", connStr))
	return db, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}


func main() {
	db, err := connectPostgres()
	if err != nil {
		panic(err)
	}
	vc := converter.NewVideoConverter(db)
	vc.Handle([]byte(`{"video_id": 1, "path": "/media/uploads/1"}`))
}
