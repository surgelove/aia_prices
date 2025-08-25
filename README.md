# aia_stream_prices

A small service that connects to brokers (currently OANDA) and streams live price data into Redis.

This project is intended for rapid prototyping and small integrations where low-latency, short-lived price messages are useful. Price messages are stored in Redis with a short TTL and an index is maintained for active messages.

## Features

- Stream live prices from OANDA (live/fxtrade endpoints)
- Publish price messages to Redis with configurable TTL
- Support for adding/removing instruments at runtime
- Lightweight helper utilities (time conversions, small TTS helper, simple movement tracker)

## Quick start

Prerequisites

- Python 3.10+ (project uses modern stdlib and typed third-party libs)
- Redis server running locally or reachable from the host
- OANDA live API key and Account ID (this project calls the live endpoints; use with care)

Basic steps

1. Install dependencies (create a venv first if desired):

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

2. Fill `config/secrets.json` with your broker credentials (see `config/secrets_example.json` for format).

3. Edit `config/main.json` to list instruments and Redis connection details.

4. Run the streamer (defaults to OANDA broker):

```bash
python3 src/main.py --broker oanda
```

## Configuration

- `config/main.json` controls runtime configuration:
  - `instruments`: list of instruments to stream (e.g., `"USD_CAD"`, `"EUR_USD"`)
  - `redis`: host/port/db
  - `ttl`: optional TTLs for `price_data` and `price_index`

- `config/secrets.json` stores broker credentials. For OANDA provide:
  - `api_key`
  - `account_id`

Example `config/secrets_example.json` is included in the repo.

## Development notes

- The code contains helpers in `src/helper.py` for small utilities.
- `src/broker.py` contains OANDA streaming and historical fetch utilities.
- `src/main.py` runs a `PriceStreamer` which manages Redis and streams prices.

Important: the project currently uses live OANDA endpoints. If you need to use the practice environment (paper trading), update the base URLs (`api-fxtrade.oanda.com` and `stream-fxtrade.oanda.com`) in `src/broker.py` to the practice endpoints as documented by OANDA.

## Extending the project

- Add support for additional brokers by implementing the same functions used by `PriceStreamer` (a stream generator yielding price dicts).
- Add unit tests for `helper.TimeBasedMovement.calc` and any other logic-heavy functions.

## Troubleshooting

- "Failed to connect to Redis": ensure `redis-server` is running and `config/main.json` points to the correct host/port.
- Auth errors from OANDA: double-check `api_key` and `account_id` in `config/secrets.json`.

## Security

- Do not commit your real `config/secrets.json` to source control. Use `config/secrets_example.json` as a template.

## License

This repository does not include a license file. Add one if you plan to open-source or share the project.
