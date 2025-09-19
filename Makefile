# Simple Makefile for the header-only rarexsec project.
# It stages the headers under build/include/ for consumption and supports
# basic install/uninstall operations.

PREFIX ?= /usr/local
DESTDIR ?=
BUILD_DIR ?= build
STAGING_INCLUDE_DIR := $(BUILD_DIR)/include
INCLUDE_SUBDIR := rarexsec
INSTALL_INCLUDE_DIR := $(DESTDIR)$(PREFIX)/include
HEADER_FILES := $(shell find include -type f \( -name '*.h' -o -name '*.hh' -o -name '*.hpp' \))

.PHONY: all build install uninstall clean distclean list

all: build

build: $(BUILD_DIR)/.stamp

$(BUILD_DIR)/.stamp: $(HEADER_FILES)
	@echo "Staging headers under $(STAGING_INCLUDE_DIR)"
	rm -rf $(BUILD_DIR)
	mkdir -p $(STAGING_INCLUDE_DIR)
	cp -R include/$(INCLUDE_SUBDIR) $(STAGING_INCLUDE_DIR)/
	touch $@

install: build
	@echo "Installing headers to $(INSTALL_INCLUDE_DIR)"
	mkdir -p $(INSTALL_INCLUDE_DIR)
	cp -R include/$(INCLUDE_SUBDIR) $(INSTALL_INCLUDE_DIR)/

uninstall:
	@echo "Removing $(INSTALL_INCLUDE_DIR)/$(INCLUDE_SUBDIR)"
	rm -rf $(INSTALL_INCLUDE_DIR)/$(INCLUDE_SUBDIR)

list:
	@printf '%s\n' $(HEADER_FILES)

clean:
	rm -f $(BUILD_DIR)/.stamp

distclean: clean
	rm -rf $(BUILD_DIR)
