// Automatically executed by ROOT on startup when run from this repository's
// directory. It makes sure the librexsec_processing library is available for
// interactive macros without requiring each macro to embed custom loading
// logic.
#include "TSystem.h"

#include <iostream>
#include <string>
#include <vector>

namespace {

bool try_load(const std::string &candidate) {
    return !candidate.empty() && gSystem->Load(candidate.c_str()) >= 0;
}

std::string determine_base_directory() {
    std::string dir = gSystem->DirName(__FILE__);
    if (dir.empty() || dir == ".") {
        if (const char *pwd = gSystem->pwd()) {
            dir = pwd;
        }
    }
    return dir;
}

void add_relative_library_paths(const std::string &base_dir) {
    if (base_dir.empty()) {
        return;
    }

    const std::vector<std::string> relative_dirs = {
        ".",
        "build",
        "build/src",
        "build-apps/src",
        "install/lib"
    };

    for (const auto &dir : relative_dirs) {
        std::string path = base_dir;
        if (!path.empty() && path.back() != '/') {
            path += '/';
        }
        path += dir;
        gSystem->AddDynamicPath(path.c_str());
    }
}

void ensure_processing_library_loaded() {
    static bool attempted = false;
    if (attempted) {
        return;
    }
    attempted = true;

    const std::vector<std::string> base_names = {
        "librarexsec_processing",
        "librarexsec_processing.so",
        "librarexsec_processing.dylib"
    };

    for (const auto &name : base_names) {
        if (try_load(name)) {
            return;
        }
    }

    if (const char *override_path = gSystem->Getenv("RAREXSEC_PROCESSING_LIBRARY")) {
        if (try_load(override_path)) {
            return;
        }
    }

    add_relative_library_paths(determine_base_directory());

    for (const auto &name : base_names) {
        if (try_load(name)) {
            return;
        }
    }

    std::cerr << "[rarexsec] Warning: unable to load librexsec_processing. "
              << "Build the project (e.g. 'make') or set RAREXSEC_PROCESSING_LIBRARY "
              << "to the full path of the compiled library." << std::endl;
}

struct Bootstrap {
    Bootstrap() { ensure_processing_library_loaded(); }
};

Bootstrap bootstrap;

} // namespace
