package logger

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
)

type PrettyHandlerOptions struct {
	SlogOpts *slog.HandlerOptions
}

type PrettyHandler struct {
	slog.Handler
	l *slog.Logger
}

func NewPrettyHandler(out io.Writer, opts *PrettyHandlerOptions) *PrettyHandler {
	h := &PrettyHandler{}

	if opts == nil {
		opts = &PrettyHandlerOptions{}
	}

	h.Handler = slog.NewTextHandler(out, opts.SlogOpts)
	h.l = slog.New(h.Handler)
	return h
}

func (h *PrettyHandler) Handle(ctx context.Context, r slog.Record) error {
	// Extract file and line number from source
	var file, line string
	if r.PC != 0 {
		frames := runtime.CallersFrames([]uintptr{r.PC})
		frame, _ := frames.Next()
		file = frame.File
		if idx := strings.LastIndexByte(file, '/'); idx >= 0 {
			file = file[idx+1:]
		}
		line = string(rune(frame.Line))
	}

	// Create a map for the formatted log
	logMap := map[string]interface{}{
		"level": r.Level.String(),
		"msg":   r.Message,
		"file":  file + ":" + line,
	}

	// Add attributes
	r.Attrs(func(a slog.Attr) bool {
		logMap[a.Key] = a.Value.Any()
		return true
	})

	// Marshal to JSON with indentation
	jsonLog, err := json.MarshalIndent(logMap, "", "  ")
	if err != nil {
		return err
	}

	// Write the log line
	_, err = os.Stdout.Write(append(jsonLog, '\n'))
	return err
}

func Init() *slog.Logger {
	handler := NewPrettyHandler(os.Stdout, &PrettyHandlerOptions{
		SlogOpts: &slog.HandlerOptions{
			Level:     slog.LevelDebug,
			AddSource: true,
		},
	})
	return slog.New(handler)
}
