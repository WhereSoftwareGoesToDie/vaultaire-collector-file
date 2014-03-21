vaultaire-collector-file: install

install: build check
	go install

build: deps
	go build

deps:
	go get

clean:
	rm -f vaultaire-collector-file

check:
	go test

