FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o mqtt-consumer .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/mqtt-consumer .

# Copy configuration files
COPY config/config.json ./config/config.json

# Create healthcheck script
RUN echo '#!/bin/sh\ncurl -f http://localhost:8080/health\n' > /healthcheck.sh && \
    chmod +x /healthcheck.sh

# Expose ports
EXPOSE 8080 9090

# Run the binary
CMD ["./mqtt-consumer"]
