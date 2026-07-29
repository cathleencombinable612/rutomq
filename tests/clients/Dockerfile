FROM golang:1.25-bookworm AS go-build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY franz_smoke.go .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/franz-smoke ./franz_smoke.go

FROM python:3.13-slim-bookworm
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir --requirement /tmp/requirements.txt
COPY librdkafka_smoke.py /opt/rutomq-clients/librdkafka_smoke.py
COPY --from=go-build /out/franz-smoke /usr/local/bin/franz-smoke
