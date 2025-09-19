CMAKE ?= cmake
BUILD_DIR ?= build
LIB_BUILD_DIR ?= build-lib
APPS_BUILD_DIR ?= build-apps
CONFIGURE_FLAGS ?=
INSTALL_FLAGS ?=

.PHONY: all configure build install test clean distclean \
configure-lib build-lib install-lib clean-lib distclean-lib \
configure-apps build-apps install-apps clean-apps distclean-apps

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

configure-lib:
	$(CMAKE) -S . -B $(LIB_BUILD_DIR) $(CONFIGURE_FLAGS) -DRAREXSEC_BUILD_LIB=ON -DRAREXSEC_BUILD_APPS=OFF

build-lib: configure-lib
	$(CMAKE) --build $(LIB_BUILD_DIR)

install-lib: build-lib
	$(CMAKE) --install $(LIB_BUILD_DIR) $(INSTALL_FLAGS)

clean-lib:
	-$(CMAKE) --build $(LIB_BUILD_DIR) --target clean

distclean-lib:
	rm -rf $(LIB_BUILD_DIR)

configure-apps:
	$(CMAKE) -S . -B $(APPS_BUILD_DIR) $(CONFIGURE_FLAGS) -DRAREXSEC_BUILD_LIB=ON -DRAREXSEC_BUILD_APPS=ON

build-apps: configure-apps
	$(CMAKE) --build $(APPS_BUILD_DIR)

install-apps: build-apps
	$(CMAKE) --install $(APPS_BUILD_DIR) $(INSTALL_FLAGS)

clean-apps:
	-$(CMAKE) --build $(APPS_BUILD_DIR) --target clean

distclean-apps:
	rm -rf $(APPS_BUILD_DIR)
