package logger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"
)

var sydneyLoc *time.Location

func init() {
	var err error
	sydneyLoc, err = time.LoadLocation("Australia/Sydney")
	if err != nil {
		// Fallback to fixed AEST offset if timezone DB unavailable
		sydneyLoc = time.FixedZone("AEST", 10*60*60)
	}
}

type PrettyHandlerOptions struct {
	SlogOpts *slog.HandlerOptions
}

type PrettyHandler struct {
	slog.Handler
	w io.Writer
}

func NewPrettyHandler(out io.Writer, opts *PrettyHandlerOptions) *PrettyHandler {
	if opts == nil {
		opts = &PrettyHandlerOptions{}
	}
	return &PrettyHandler{
		Handler: slog.NewTextHandler(out, opts.SlogOpts),
		w:       out,
	}
}

func (h *PrettyHandler) Handle(ctx context.Context, r slog.Record) error {
	var file string
	var line int
	if r.PC != 0 {
		frames := runtime.CallersFrames([]uintptr{r.PC})
		frame, _ := frames.Next()
		file = frame.File
		if idx := strings.LastIndexByte(file, '/'); idx >= 0 {
			file = file[idx+1:]
		}
		line = frame.Line
	}

	timestamp := r.Time.In(sydneyLoc).Format("2006-01-02 15:04:05.000 MST")

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s  %-7s  [%s:%d]  %s", timestamp, r.Level.String(), file, line, r.Message)

	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&buf, "  %s=%v", a.Key, a.Value.Any())
		return true
	})
	buf.WriteByte('\n')

	_, err := h.w.Write(buf.Bytes())
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
