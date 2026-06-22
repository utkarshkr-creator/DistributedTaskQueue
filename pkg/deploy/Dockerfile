# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build
WORKDIR /src

# Cache module downloads separately from source changes
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/queue-app ./cmd/main
# NOTE: adjust the build path above (./cmd/main) to wherever your main
# package actually lives in the module — this assumes a cmd/main layout.
# If main.go sits at the module root, use: go build -o /out/queue-app .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/queue-app .

EXPOSE 9090
ENTRYPOINT ["./queue-app"]
