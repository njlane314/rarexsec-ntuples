CMAKE ?= cmake
BUILD_DIR ?= build
CONFIGURE_FLAGS ?=
INSTALL_FLAGS ?=

.PHONY: all configure build install test clean distclean

all: build

configure:
	$(CMAKE) -S . -B $(BUILD_DIR) $(CONFIGURE_FLAGS)

build: configure
	$(CMAKE) --build $(BUILD_DIR)

install: build
	$(CMAKE) --install $(BUILD_DIR) $(INSTALL_FLAGS)

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure

clean:
	-$(CMAKE) --build $(BUILD_DIR) --target clean

distclean:
	rm -rf $(BUILD_DIR)
