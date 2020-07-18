FROM golang:alpine
RUN apk --no-cache add tzdata
RUN mkdir /app
ADD . /app/
WORKDIR /app
RUN go build -o main .
RUN adduser -S -D -H -h /app appuser
USER appuser
CMD ["./main"]