# syntax=docker/dockerfile:1

ARG GO_VERSION=1.25
ARG ALPINE_VERSION=3.20

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

ARG APP=edge-gateway
ARG TARGETOS=linux
ARG TARGETARCH

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN case "${APP}" in \
      edge-gateway|private-connector|signurl) ;; \
      *) echo "unsupported APP=${APP}" >&2; exit 1 ;; \
    esac \
 && targetos="${TARGETOS:-linux}" \
 && targetarch="${TARGETARCH:-$(go env GOARCH)}" \
 && CGO_ENABLED=0 GOOS="${targetos}" GOARCH="${targetarch}" \
      go build -trimpath -ldflags="-s -w" -o /out/air3 "./cmd/${APP}"

FROM alpine:${ALPINE_VERSION}
RUN adduser -D -H -u 10001 appuser
COPY --from=build /out/air3 /usr/local/bin/air3
USER appuser

# The selected APP binary is copied to this stable runtime path.
CMD ["/usr/local/bin/air3"]
