FROM golang:1.19.0 as build
WORKDIR /workspace
COPY go.mod go.sum /workspace/
RUN go mod download
COPY * /workspace/
COPY fs /workspace/fs/
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags '-s -w -buildid=' -trimpath -o /main main.go

# distrolessは当然、fuseが使えないので ~~alpineで試す~~
# https://github.com/gliderlabs/docker-alpine/issues/268#issuecomment-297043078
#FROM alpine:3.16.2
FROM ubuntu:20.04
COPY --from=build /main .
RUN apt-get update
RUN apt-get install fuse libfuse2
COPY resource/docker/fuse.conf /etc/fuse.conf
CMD ["/main"]
