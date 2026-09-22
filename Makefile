PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ducklake
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Core extensions that we need for crucial testing
DEFAULT_TEST_EXTENSION_DEPS=
# For cloud testing we also need these extensions
FULL_TEST_EXTENSION_DEPS=httpfs

# Stabilize all tests in CI
ifdef CI
TEST_FLAGS:=--stabilize-tests
endif
# ~[.] excludes hidden/slow (.test_slow) tests
T ?= $(TEST_FLAGS) --test-dir "$(PROJ_DIR)" --test-config "$(PROJ_DIR)test/configs/managed.json" "~[.]test/*"

# Aws and Azure have vcpkg dependencies and therefore need vcpkg merging
ifeq (${BUILD_EXTENSION_TEST_DEPS}, full)
	USE_MERGED_VCPKG_MANIFEST:=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Pass the managed contract to the extension-ci-tools test targets.
TEST_PATH = /test/unittest --test-dir "$(PROJ_DIR)" --test-config "$(PROJ_DIR)test/configs/managed.json"
TESTS_BASE_DIRECTORY = ~[.]test/

unittest_relassert:
	python3 duckdb/scripts/ci/run_tests.py build/relassert/test/unittest --batch-timeout 900 --max-retries 10 $(T)
