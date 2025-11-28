export GOPROXY := https://proxy.golang.org,direct
export GOPRIVATE := gitlab.playcourt.id/telkom-digital/dpe/modules/go/pii-encryption
export PATH := $(PWD)/bin:/usr/local/go/bin:$(PATH)


help:
	@echo "See Makefile"
tidy:
	@go mod tidy
update-all-module:
	@go get -u ./...
run-example:
	@go run example/logger.go
run-example-datadog:
	@go run example/datadog/datadog.go
scan:
	@rm -f bin/gosec
	@if ! [ -x "$$(command -v gosec)" ]; then\
        echo "install gosec binary now ...";\
		GOBIN=$(PWD)/bin/ go install github.com/securego/gosec/v2/cmd/gosec@latest;\
    fi

	@if [ -x "$$(command -v gosec)" ]; then\
        gosec ./...;\
        rm -f bin/gosec;\
    fi
