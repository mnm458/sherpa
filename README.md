# Sherpa

Sherpa is a Go trading bot that receives webhook signals from TradingView and executes leveraged futures orders on Bybit and Binance. When a take-profit fills, it automatically re-enters the same position at the live market price.


## Exchanges

| Exchange | Re-entry mechanism |
|----------|--------------------|
| Bybit    | Native private WebSocket (`/v5/private`) — watches order updates directly |
| Binance  | Futures user-data stream + mark-price stream to confirm price has moved away from TP |

## Signal format

Paste these as TradingView alert message bodies.

### Bybit

```json
{
  "category": "linear",
  "symbol": "BTCUSDT",
  "side": "Buy",
  "order_type": "Limit",
  "position_idx": 0,
  "leverage": 5,
  "tp": 0.015,
  "sl": 0.010
}
```

`tp` and `sl` are fractional offsets from the entry price (`0.015` = 1.5%).

### Binance

```json
{
  "symbol": "BTCUSDT",
  "type": "LIMIT",
  "action": "Buy",
  "leverage": 5,
  "tp": 0.015,
  "sl": 0.010
}
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/ping` | Health check — returns `OK` |
| `GET` | `/status` | Full system snapshot (JSON) |
| `POST` | `/handle-signal` | Ingest a TradingView signal |
| `POST` | `/test` | Fire a hardcoded Bybit test order |
| `POST` | `/test-binance` | Fire a hardcoded Binance test order |
| `POST` | `/adhoc-market` | Place an ad-hoc Binance market order |

### `/status` response

```json
{
  "healthy": true,
  "exchange": "BYBIT",
  "environment": "PROD",
  "uptime": "4h32m10s",
  "reEntryOn": true,
  "signalInFlight": false,
  "websocket": {
    "connected": true,
    "authenticated": true,
    "lastMessageAt": "2026-05-31 14:22:01.123 AEST",
    "lastPingAt": "2026-05-31 14:22:00.000 AEST"
  },
  "currentPosition": {
    "hasPosition": true,
    "symbol": "BTCUSDT",
    "side": "Buy",
    "quantity": 0.003,
    "entryPrice": 67500.0,
    "takeProfit": 68512.5,
    "stopLoss": 66825.0,
    "leverage": 5
  },
  "lastSignalAt": "2026-05-31 14:20:00.000 AEST"
}
```

HTTP 200 = healthy. HTTP 503 = unhealthy (WebSocket down while `reEntrySwitch` is on).

### `/adhoc-market` request body

```json
{
  "symbol": "BTCUSDT",
  "side": "Sell",
  "quantity": "0.005"
}
```

## Environment variables

Create a `.env` file in each instance directory (see deployment section).

| Variable | Exchange | Env | Description |
|----------|----------|-----|-------------|
| `BYBIT_API_KEY_TEST` | Bybit | Testnet | API key |
| `BYBIT_SECRET_TEST` | Bybit | Testnet | API secret |
| `BYBIT_BASE_URL_TEST` | Bybit | Testnet | Base URL |
| `BYBIT_API_KEY_PROD` | Bybit | Prod | API key |
| `BYBIT_SECRET_PROD` | Bybit | Prod | API secret |
| `BYBIT_BASE_URL_PROD` | Bybit | Prod | Base URL |
| `BYBIT_WS_PRIVATE_PROD` | Bybit | Prod | Private WebSocket URL (`wss://stream.bybit.com/v5/private`) |
| `BINANCE_API_KEY_TEST` | Binance | Testnet | API key |
| `BINANCE_SECRET_TEST` | Binance | Testnet | API secret |
| `BINANCE_BASE_URL_TEST` | Binance | Testnet | Base URL |
| `BINANCE_API_KEY_PROD` | Binance | Prod | API key |
| `BINANCE_SECRET_PROD` | Binance | Prod | API secret |
| `BINANCE_BASE_URL_PROD` | Binance | Prod | Base URL |

## CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `-exchange` | required | `bybit` or `binance` |
| `-env` | required | `test` or `prod` |
| `-addr` | `:4000` | HTTP listen address |
| `-reEntrySwitch` | `false` | Enable WebSocket re-entry on TP fill |

## Build & run

```bash
git clone --recurse-submodules git@github.com:mnm458/sherpa.git
cd sherpa
go mod tidy
go build -o sherpa ./cmd/web

# Bybit, prod, re-entry on
./sherpa -exchange bybit -env prod -addr :4000 -reEntrySwitch=true

# Binance, prod, re-entry off
./sherpa -exchange binance -env prod -addr :4000
```

## Test

```bash
go test ./...
```
