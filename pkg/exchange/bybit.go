package exchange

type BybitSignal struct{}

type BybitHandler struct{}

func NewBybitHandler(apiKey string, secret string) *BybitHandler {
	return &BybitHandler{}
}

func (bh *BybitHandler) Validate() error {
	return nil
}

func (bh *BybitHandler) Process(s Signal) error {
	return nil
}
