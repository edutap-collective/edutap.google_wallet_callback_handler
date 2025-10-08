# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a FastAPI-based callback handler for Google Wallet pass events (register/unregister). It receives callbacks from Google Wallet when passes are added to or removed from user devices, validates signed messages, and forwards events to Apache Kafka for downstream processing.

The service is part of the eduTAP ecosystem and implements the Google Wallet callback API specification.

## Architecture

### Core Components

**FastAPI Application** ([main.py](src/edutap/google_wallet_callback_handler/main.py))
- Entry point with lifespan management for initialization/shutdown
- Integrates the callback router from `edutap.wallet_google.handlers.fastapi` at `/v1` prefix
- Root path is `/wallet/google` (configured for deployment behind a reverse proxy)
- Includes Sentry integration for error tracking
- Uses uvloop for improved async performance
- Health check endpoint at `HEAD /` that validates Kafka connectivity

**Callback Handler** ([kafka.py](src/edutap/google_wallet_callback_handler/kafka.py))
- `KafkaCallbackHandler` implements the `edutap.wallet_google.protocols.CallbackHandler` protocol
- Receives validated callback data (class_id, object_id, event_type, etc.)
- Produces messages to Kafka topic with object_id as key
- Thread-local Kafka producer management via `KafkaSessionManager`
- Supports both plaintext and mTLS connections

**Settings Management** ([settings.py](src/edutap/google_wallet_callback_handler/settings.py))
- Uses Pydantic Settings with `.env` file support
- All settings prefixed with `EDUTAP_` (Kafka settings use `EDUTAP_KAFKA_` prefix)
- Key settings: `ENVIRONMENT` (production/development/testing), `GOOGLE_CALLBACK_URL`, `SENTRY_DSN`

**Alternative Handler** ([filelogger.py](src/edutap/google_wallet_callback_handler/filelogger.py))
- Simple file-based callback handler that writes to `/logs/callback.log`
- Useful for debugging or environments without Kafka

### Message Flow

1. Google Wallet sends signed POST request to `/wallet/google/v1/callback`
2. Router from `edutap.wallet_google` validates JWT signature
3. Validated data passed to callback handler
4. `KafkaCallbackHandler` serializes `SignedMessage` model to JSON and sends to Kafka
5. Message published to `edutap.google_callback` topic with object_id as partition key

### Deployment Architecture

The service runs in a Docker Compose stack with:
- **callback-handler**: FastAPI service (port 8085 internally, exposed via Traefik)
- **kafka**: Apache Kafka (official `apache/kafka:latest` image) with KRaft (no Zookeeper)
- **kafka-ui**: Web interface at `/kafka-ui`
- **traefik**: Reverse proxy with Let's Encrypt SSL
- **create-topics**: Init container that creates required Kafka topics

## Development Commands

### Local Development

```bash
# Install package with Kafka support
pip install -e ".[kafka]"

# Install with all dev tools
pip install -e ".[kafka,dev,test,typecheck]"

# Run the service locally
python -m edutap.google_wallet_callback_handler.main
# Or use the installed console script:
google-wallet-callback-handler
```

### Testing

```bash
# Run tests (pytest configured in pyproject.toml)
pytest

# Lint with flake8
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

# Type checking
mypy src/
```

### Code Formatting

```bash
# Format with black
black src/

# Sort imports (isort configured with black profile)
isort src/
```

## Docker Commands

### Build and Run

```bash
# Build image
docker build -t edutap-callback-handler .

# Run with docker-compose (requires .env file)
docker-compose up

# Run in background
docker-compose up -d

# View logs
docker-compose logs -f callback-handler

# Rebuild after code changes
docker-compose up --build
```

### Environment Setup

Create `.env` file in project root:

```env
DNS=192.168.8.1
DOMAIN=demo.edutap.eu
HTTPS_PORT=443
HTTP_PORT=8086
EDUTAP_ENVIRONMENT=development
EDUTAP_SENTRY_DSN=<your-sentry-dsn>
```

Ensure:
- Domain points to your server
- Port 80 accessible for Let's Encrypt validation
- HTTPS_PORT is available
- `./letsencrypt` directory exists with correct permissions

**Note**: Kafka data is stored in a Docker named volume (`kafka_data`) which handles permissions automatically. No manual directory setup required. If migrating from an old setup, remove the old `./kafka_data` directory first.

### Access Deployed Services

- Swagger UI: `https://${DOMAIN}:${HTTPS_PORT}/docs` (development only)
- Kafka UI: `https://${DOMAIN}:${HTTPS_PORT}/kafka-ui`
- Callback endpoint: `https://${DOMAIN}:${HTTPS_PORT}/wallet/google/v1/callback`
- Health check: `https://${DOMAIN}:${HTTPS_PORT}/`

## Important Implementation Details

### Kafka Configuration

- **Image**: Official Apache Kafka (`apache/kafka:latest`) with KRaft mode
- **Topic**: `edutap.google_callback` (created by init container)
- **Message Key**: object_id (ensures ordering per pass object)
- **Message Value**: JSON-serialized `SignedMessage` model
- **Connection**: Supports both plaintext and mTLS (configure via `EDUTAP_KAFKA_*` env vars)
- **Bootstrap Servers**: Default is `kafka:9092` (internal Docker network)
- **Storage**: Uses Docker named volume `kafka_data` for persistence

### Thread Safety

The Kafka producer uses thread-local storage (`threading.local()`) to ensure thread safety in the FastAPI async environment. One producer instance per thread.

### Security Considerations

- Callback signature validation is handled by `edutap.wallet_google` library
- OpenAPI docs disabled in production (controlled by `ENVIRONMENT` setting)
- Sentry PII collection enabled (for debugging user-specific issues)
- Kafka UI currently unprotected (TODO in README)

### Dependencies

Critical external dependencies:
- `edutap-wallet-google[callback]>=1.0.0b1` - Provides callback router and validation
- `aiokafka` - Async Kafka client
- `fastapi>=0.115.7` and `uvicorn[standard]>=0.34.0` - Web framework
- `pydantic-settings` - Configuration management

## Project Structure

```
src/edutap/google_wallet_callback_handler/
├── main.py          # FastAPI app, lifespan, entry point
├── kafka.py         # Kafka producer and callback handler
├── filelogger.py    # Alternative file-based handler
├── settings.py      # Pydantic settings (main app)
└── log.py           # Structured logging setup
```

## Console Scripts

The package installs `google-wallet-callback-handler` command that runs `main:main()` (starts uvicorn on 0.0.0.0:8085).

## CI/CD

GitHub Actions workflows:
- **python-package.yml**: Tests on Python 3.10-3.13, runs flake8 and pytest
- **docker-image.yml**: Builds Docker image
- **docker-publish.yml**: Publishes to GitHub Container Registry

## Python Version Support

Requires Python 3.10-3.13 (specified in pyproject.toml).