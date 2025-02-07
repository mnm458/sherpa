package exchange

import "errors"

const (
	ACTION_LONG = "long"
	TYPE_OPEN   = "open"
)

var (
	errInvalidSignal             = errors.New("invalid signal")
	errInvalidSide               = errors.New("invalid side")
	errPriceRespUnmarshalFailure = errors.New("failed to unmarshal price server response")
	errUnsupportedSide           = errors.New("unsupported side, only Buy or Sell valid")
)
