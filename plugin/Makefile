# Makefile for building the qv-lite C plugin library.
#
# Targets:
#   make all          — build both shared and static libraries
#   make shared       — build shared library (.so / .dll / .dylib)
#   make static       — build static library (.a)
#   make test         — build the test/example program
#   make clean        — remove build artifacts
#
# Requirements: Go 1.24+, a C compiler (gcc/clang).

# Detect OS
UNAME_S := $(shell uname -s 2>/dev/null || echo Windows)

ifeq ($(UNAME_S),Linux)
	SHARED_EXT  = so
	STATIC_EXT  = a
	SHARED_NAME = libqv_lite.so
	STATIC_NAME = libqv_lite.a
	HEADER_NAME = libqv_lite.h
endif
ifeq ($(UNAME_S),Darwin)
	SHARED_EXT  = dylib
	STATIC_EXT  = a
	SHARED_NAME = libqv_lite.dylib
	STATIC_NAME = libqv_lite.a
	HEADER_NAME = libqv_lite.h
endif
ifeq ($(UNAME_S),Windows)
	SHARED_EXT  = dll
	STATIC_EXT  = lib
	SHARED_NAME = qv_lite.dll
	STATIC_NAME = qv_lite.lib
	HEADER_NAME = qv_lite.h
endif

GO_BUILD = go build
CGO_ENABLED = CGO_ENABLED=1
BUILD_DIR = build

.PHONY: all shared static clean test header

all: shared static header

# ---- Shared library ----
shared:
	$(CGO_ENABLED) $(GO_BUILD) -buildmode=c-shared -o $(BUILD_DIR)/$(SHARED_NAME) .

# ---- Static library ----
# c-archive mode produces a .a file; header must be copied separately.
static:
	$(CGO_ENABLED) $(GO_BUILD) -buildmode=c-archive -o $(BUILD_DIR)/$(STATIC_NAME) .

# ---- Header ----
# Copy the hand-written header alongside the c-archive one for convenience.
header:
	@mkdir -p $(BUILD_DIR)
	cp qv_lite.h $(BUILD_DIR)/$(HEADER_NAME)

# ---- Test / example program ----
test: shared
	$(CC) -o $(BUILD_DIR)/example example/example.c -I$(BUILD_DIR) -L$(BUILD_DIR) -lqv_lite -lpthread

clean:
	rm -rf $(BUILD_DIR)
