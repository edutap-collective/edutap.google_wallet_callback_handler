# syntax=docker/dockerfile:1
# Two stages: the build installs the package, the runtime image carries only the
# result. Plain `pip install` on purpose -- `uv` belongs in the development
# environment, not in a container image.
FROM python:3.14-slim AS build
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
# `.` and not `.[kafka]`: aiokafka is an ordinary dependency now. The extra
# promised a choice this package never offered, since `main` imports `kafka`,
# which imports `aiokafka` at module level.
RUN pip install --no-cache-dir .

FROM python:3.14-slim
# The interpreter of the base image is 3.14, so this is where `pip install` put
# the package in the build stage. Changing the base image tag means changing
# these two paths with it.
COPY --from=build /usr/local/lib/python3.14/site-packages /usr/local/lib/python3.14/site-packages
COPY --from=build /usr/local/bin /usr/local/bin

# 8086 is the port the stack routes to; the deployment overrides it either way.
ARG HTTP_PORT=8086
ENV HTTP_PORT=${HTTP_PORT}

RUN useradd --create-home --uid 10001 app
WORKDIR /app
USER app
EXPOSE ${HTTP_PORT}

# `--proxy-headers` because Traefik terminates TLS in front of this and the
# service has to see the original scheme and host.
CMD ["sh", "-c", "uvicorn edutap.wallet_google_callback_handler.main:app --proxy-headers --host 0.0.0.0 --port $HTTP_PORT --access-log"]
