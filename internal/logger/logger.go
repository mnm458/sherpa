package logger

import (
	"log/slog"
	"os"
)

func Init() *slog.Logger {
	loggerHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	})
	return slog.New(loggerHandler)
}
