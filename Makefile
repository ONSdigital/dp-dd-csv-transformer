build:
	go build -o build/dp-csv-transformer

debug: build
	HUMAN_LOG=1 ./build/dp-csv-transformer

.PHONY: build debug
