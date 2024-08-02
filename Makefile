VERSION = $(shell (git describe --tags --abbrev=0 2>/dev/null || echo '0.0.0') | tr -d v)
COMMIT = $(shell git rev-parse --short HEAD)

.PHONY: info
info:
	@echo "VERSION: $(VERSION)"
	@echo "COMMIT: $(COMMIT)"

unit-test:
	@cargo t

integration-test:
	@cargo b
	@bash ./src/bin/feacli/tests/tests.sh

test: unit-test integration-test
