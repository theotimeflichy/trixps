# TrixPS

TrixPS is a distributed pubsub server.
Writen in Go.
Deployed using Docker

## How to run

### Launch the brokers
```bash
cd deployment
docker compose build
docker compose up -d broker1 broker2 broker3
```

### Start clients
```bash
# On one bash to listen
# --topic is the name of the topic you want to listen
docker compose run --rm consumer --brokers broker1:9001,broker2:9002,broker3:9003 --topic sport --follow

# On another bash to send
# --topic is the name of the topic you want to listen
docker compose run --rm -it producer --brokers broker1:9001,broker2:9002,broker3:9003 --topic sport -i
```

### Check health
```bash
docker compose run --rm producer --brokers broker1:9001,broker2:9002,broker3:9003 --status
```
