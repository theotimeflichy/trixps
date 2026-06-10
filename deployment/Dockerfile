# Start from an image which has go setted-up
FROM golang:1.22 AS build

# Go to src folder
WORKDIR /src

# Move the dependency file
COPY go.mod go.sum ./

# Dowload dependency
RUN go mod download

# Move the rest of source code
COPY . .

# Build the 4 apps (broker, producer, consumer, api)
RUN CGO_ENABLED=0 go build -o /out/broker   ./cmd/broker \
 && CGO_ENABLED=0 go build -o /out/producer ./cmd/producer \
 && CGO_ENABLED=0 go build -o /out/consumer ./cmd/consumer \
 && CGO_ENABLED=0 go build -o /out/api      ./cmd/api

# Use alpine to have a lighter image
FROM alpine:3.20

# Move the compiled apps
COPY --from=build /out/broker /out/producer /out/consumer /out/api /usr/local/bin/

# no entrypoint because multiple programs
ENTRYPOINT []
