package converter

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"
)
func IsProcessed(db *sql.DB, videoID int) bool {
	var isProcessed bool
	query := "SELECT EXISTS(SELECT 1 FROM processed_videos WHERE video_id = $1 AND status = 'success')"
	err := db.QueryRow(query, videoID).Scan(&isProcessed)
	if err != nil {
		slog.Error("failed to check if video is processed", slog.Int("video_id", videoID))
		return false
	}
	return isProcessed
}

func MarkAsProcessed(db *sql.DB, videoID int) error {
	query := "INSERT INTO processed_videos (video_id, status, processed_at) VALUES ($1, $2, $3)"
	_, err := db.Exec(query, videoID, "success", time.Now())
	if err != nil {
		slog.Error("failed to mark video as processed", slog.Int("video_id", videoID))
	}
	return nil
}

func RegisterError(db *sql.DB, errorData map[string]interface{}, err error){
	serializedError, _ := json.Marshal(errorData)
	query := "INSERT INTO process_erros_log (error_details created_at) VALUES ($1, $2,)"
	_,dbErr := db.Exec(query, serializedError, time.Now())
	if dbErr != nil {
		slog.Error("failed to register error", slog.String("error_details", string(serializedError)))
	}
}