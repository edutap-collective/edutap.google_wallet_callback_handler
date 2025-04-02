# syntax=docker/dockerfile:1
FROM python:3.13

ARG HTTP_PORT=8085
ENV HTTP_PORT=${HTTP_PORT}

RUN echo "$HTTP_PORT"
RUN echo "$HTTP_PORT"
RUN mkdir /logs
RUN touch /logs/callback.log
RUN chmod 0777 /logs/callback.log

WORKDIR /app

COPY src /app/src
COPY pyproject.toml /app

# RUN python3 -m venv venv
RUN pip install --no-cache-dir -e "/app[kafka]"

CMD ["sh", "-c", "uvicorn edutap.google_wallet_callback_handler.main:app --proxy-headers --host 0.0.0.0 --port $HTTP_PORT --access-log"]
