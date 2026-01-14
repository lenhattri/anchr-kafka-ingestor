FROM golang:1.22-alpine AS build

RUN apk add --no-cache ca-certificates git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /anchr-kafka-ingestor ./cmd/anchr-kafka-ingestor

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /anchr-kafka-ingestor /anchr-kafka-ingestor
USER nonroot:nonroot
ENTRYPOINT ["/anchr-kafka-ingestor"]
