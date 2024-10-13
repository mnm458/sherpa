package main

import (
	"context"
	"log/slog"
)

type application struct {
	ctx    context.Context
	logger *slog.Logger
}

func NewApplication(ctx context.Context, logger *slog.Logger) *application {
	return &application{ctx: ctx, logger: logger}
}
