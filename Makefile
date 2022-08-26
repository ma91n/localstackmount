install:
	# https://goreleaser.com
	go install github.com/goreleaser/goreleaser@v1.10.3

release:
	goreleaser release --snapshot --rm-dist
