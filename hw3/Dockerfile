FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /kvraft-server ./server

FROM alpine:3.20
COPY --from=builder /kvraft-server /kvraft-server
ENTRYPOINT ["/kvraft-server"]
