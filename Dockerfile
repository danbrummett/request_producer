FROM alpine

WORKDIR /app
ADD . ./
ENTRYPOINT ["./request_producer"]
