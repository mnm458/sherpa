package exchange

import "errors"

const (
	ACTION_LONG = "long"
	TYPE_OPEN   = "open"
)

var (
	errInvalidSignal              = errors.New("invalid signal")
	errInvalidSide                = errors.New("invalid side")
	errInvalidServerResp          = errors.New("invalid wallet balance response")
	errWalletRespUnmarshalFailure = errors.New("failed to unmarshal wallet server response")
	errPriceRespUnmarshalFailure  = errors.New("failed to unmarshal price server response")
)
