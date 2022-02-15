FROM golang:1.17.7

WORKDIR /workspace

# install dependencies and download them
COPY go.mod go.sum ./
RUN go mod download

# copy files
COPY ./main.go ./
COPY ./api ./api
COPY ./app ./app

RUN CGO_ENABLED=0 GOOS=linux go build -o glowmarkt-importer ./

FROM alpine:3.15.0

WORKDIR /tmp

COPY --from=0 /workspace/glowmarkt-importer /usr/bin

USER nobody

ENTRYPOINT ["glowmarkt-importer"]
