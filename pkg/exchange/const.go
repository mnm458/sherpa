package exchange

import "errors"

const (
	ACTION_LONG = "long"
	TYPE_OPEN   = "open"
)

var (
	errInvalidSignal = errors.New("invalid signal")
)
