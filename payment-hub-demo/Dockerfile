FROM golang:1.23-alpine AS builder
ENV GOTOOLCHAIN=auto
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o payment-hub-demo .

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/payment-hub-demo .
USER 65532:65532
ENTRYPOINT ["/payment-hub-demo"]
