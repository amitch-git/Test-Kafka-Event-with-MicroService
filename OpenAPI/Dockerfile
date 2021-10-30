FROM golang:latest
RUN mkdir .src/app
WORKDIR /src/app
COPY . .
RUN chmod 755 /src/app/main
EXPOSE 8080
CMD ["./main"]