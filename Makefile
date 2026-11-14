PS5_PAYLOAD_SDK ?= /opt/ps5-payload-sdk
PS5_HOST ?= ps5
PS5_PORT ?= 9021
export PS5_PAYLOAD_SDK PS5_HOST PS5_PORT

.PHONY: all build m1 deploy deploy-m1 verify-m1 test clean

all: build

build:
	./scripts/build-ps5.sh ps5-agent

m1:
	./scripts/build-ps5.sh m1-ps5-probe

deploy: build
	./scripts/deploy-ps5.sh build/ps5-agent.elf

deploy-m1: m1
	./scripts/deploy-ps5.sh build/m1-ps5-probe.elf

verify-m1:
	python3 ./scripts/verify-on-ps5.py --host "$(PS5_HOST)" --elfldr-port "$(PS5_PORT)" --mode m1 --elf build/m1-ps5-probe.elf --quit

test:
	cargo test --workspace

clean:
	cargo clean
	rm -rf build
	rm -rf bins/m1-ps5-probe/target
