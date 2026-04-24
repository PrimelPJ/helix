FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN go build -o /bin/helixd ./cmd/helixd
RUN go build -o /bin/helixctl ./cmd/helixctl

FROM alpine:3.19
COPY --from=build /bin/helixd /bin/helixd
COPY --from=build /bin/helixctl /bin/helixctl
ENTRYPOINT ["/bin/helixd"]
