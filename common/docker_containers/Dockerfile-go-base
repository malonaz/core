FROM alpine:latest
RUN adduser -s /bin/false -D app
RUN apk update && apk upgrade && apk add ca-certificates musl musl-utils && rm -rf /var/cache/apk/*
RUN apk --update add ca-certificates

RUN ln -s /lib/ld-musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2

# certs
COPY ca.crt .
COPY server.crt .
COPY server.key .
COPY client.crt .
COPY client.key .