package converter

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

type VideoConverter struct {
	db *sql.DB
}

func NewVideoConverter(db *sql.DB) *VideoConverter {
	return &VideoConverter{
		db: db,
	}
}

type VideoTask struct {
	VideoID int `json:"video_id"`
	Path string `json:"path"`
}

func (vc *VideoConverter) Handle(msg []byte) {
	var task VideoTask
	err := json.Unmarshal(msg, &task)
	if err != nil {
		panic(err)
	}
	
	if IsProcessed(vc.db, task.VideoID) {
		slog.Warn("video already processed", slog.Int("video_id", task.VideoID))
		return
	}
	err = vc.processVideo(&task)
	if err != nil {
		vc.logError(task, "failed to process video", err)
		return 
	}

	err = MarkAsProcessed(vc.db, task.VideoID)
	if err != nil {
		vc.logError(task, "failed to mark video as processed", err)
		return
	}
	slog.Info("Video marked as processed", slog.Int("video_id", task.VideoID))
}

func (vc *VideoConverter) processVideo(task *VideoTask) error {
	mergedFile := filepath.Join(task.Path, "merged.mp4")
	mpegDashPath := filepath.Join(task.Path, "mpeg-dash")

	slog.Info("Processing chunks", slog.String("path", task.Path))
	err := vc.mergeChunks(task.Path, mergedFile)
	if err != nil {
		vc.logError(*task, "failed to merge chunks", err)
		return err
	}

	slog.Info("Creating mpeg-dash", slog.String("path", task.Path))
	err = os.MkdirAll(mpegDashPath, os.ModePerm)
	if err != nil {
		vc.logError(*task, "failed to create mpeg-dash directory", err)
		return err
	}

	slog.Info("Convert to mpeg-dash", slog.String("path", task.Path))
	ffmpegCmd := exec.Command(
		"ffmpeg","-i", mergedFile,
		"-f","dash",
		filepath.Join(mpegDashPath, "output.mpd"),
	)
	output, err := ffmpegCmd.CombinedOutput()
	if err != nil {
		vc.logError(*task, "failed to convert to mpeg-dash"+string(output), err)
		return err
	}
	slog.Info("Video processing completed", slog.String("path", task.Path))

	slog.Info("Removing merged file", slog.String("path", mergedFile))
	err = os.Remove(mergedFile)
	if err != nil {
		vc.logError(*task, "failed to remove merged file", err)
		return err
	}
	return nil

}

func (vc *VideoConverter) logError(task VideoTask,message string, err error) {
	errorData:= map[string]any{
		"video_id": task.VideoID,
		"error": message,
		"details": err.Error(),
		"time": time.Now(),
	}
	serializedError, _ := json.Marshal(errorData)
	slog.Error("Processing error",slog.String("error_details", string(serializedError)))


	RegisterError(vc.db, errorData, err)

}


func (vc *VideoConverter)  extractNumber(fileName string) int {
	re:= regexp.MustCompile(`\d+`)
	numStr := re.FindString(filepath.Base(fileName))
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return -1
	}
	return num
}

func (vc *VideoConverter)  mergeChunks(inputFile string, outputFile string) error {
	chunks , err := filepath.Glob(filepath.Join(inputFile, "*.chunk"))
	if err != nil {
		return fmt.Errorf("failed to list chunks: %w", err)
	}

	sort.Slice(chunks, func(i,j int)bool {
		return vc.extractNumber(chunks[i]) < vc.extractNumber(chunks[j])
	})

	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer output.Close()

	for _, chunk := range chunks {
		input, err := os.Open(chunk)
		if err != nil {
			return fmt.Errorf("failed to open chunk: %w", err)
		}

		_, err = output.ReadFrom(input)
		if err != nil {
			return fmt.Errorf("failed to read chunk: %w", err)
		}
		input.Close()
	}

	return nil
}