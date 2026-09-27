.PHONY: test test-race load-test

test:
	go test ./...

test-race:
	go test -race -count=1 ./...

load-test:
	./scripts/load-test.sh
