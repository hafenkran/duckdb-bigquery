MAKEFILE_DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=bigquery
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: docker-build
docker-build:
	docker build -t duckdb-bigquery:v1.2.0 -f dev/Dockerfile .

.PHONY: lint
lint:
	python3 ./scripts/run-clang-tidy.py $(MAKEFILE_DIR)/src/* \
		-config-file ./.clang-tidy \
		-extra-arg-before=-std=c++11 \
		-header-filter="src/include/*.\(h|hpp)" \
		-j 4 \
		-p=build/debug/

.PHONY: cmake-format
cmake-format:
	cmake-format -c $(MAKEFILE_DIR)/.cmake-format.yaml \
		-i $(MAKEFILE_DIR)/CMakeLists.txt $(MAKEFILE_DIR)/external/CMakeLists.txt
