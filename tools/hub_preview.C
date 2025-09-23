// Run with: root -l -q 'tools/hub_preview.C'

#include "../include/rarexsec/HubDataFrame.h"

#include "TInterpreter.h"
#include "TSystem.h"

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDFHelpers.hxx"

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

void add_relative_library_paths(const std::filesystem::path &repo_root) {
    if (repo_root.empty()) {
        return;
    }

    const std::vector<std::string> relative_dirs = {
        ".",
        "build",
        "build/src",
        "build/lib",
        "build-apps",
        "build-apps/src",
        "build-apps/lib",
        "build-lib",
        "build-lib/src",
        "build-lib/lib",
        "install/lib"
    };

    for (const auto &dir : relative_dirs) {
        const std::filesystem::path candidate = repo_root / dir;
        const std::string path = candidate.string();
        if (path.empty()) {
            continue;
        }

        if (!gSystem->AccessPathName(path.c_str(), kFileExists)) {
            gSystem->AddDynamicPath(path.c_str());
        }
    }
}

bool ensure_processing_library_available() {
    static bool initialised = false;
    static bool available = false;
    if (initialised) {
        return available;
    }
    initialised = true;

    const char *candidates[] = {"librarexsec_processing", "librarexsec_processing.so",
                                "librarexsec_processing.dylib"};
    for (const auto *name : candidates) {
        if (gSystem->Load(name) >= 0) {
            available = true;
            return true;
        }
    }

    if (const char *override_path = gSystem->Getenv("RAREXSEC_PROCESSING_LIBRARY")) {
        if (gSystem->Load(override_path) >= 0) {
            available = true;
            return true;
        }
    }

    std::filesystem::path current_file = std::filesystem::absolute(__FILE__);
    auto repo_root = current_file.parent_path().parent_path();
    add_relative_library_paths(repo_root);

    for (const auto *name : candidates) {
        if (gSystem->Load(name) >= 0) {
            available = true;
            return true;
        }
    }

    auto include_dir = repo_root / "include";
    gInterpreter->AddIncludePath(include_dir.string().c_str());

    std::ostringstream declaration;
    declaration << "#undef HUB_DATAFRAME_H\n";
    declaration << "#undef RAREXSEC_DETAIL_HUBDATAFRAMEIMPL_H\n";
    declaration << "#define RAREXSEC_HEADER_ONLY 1\n";
    declaration << "#include \"rarexsec/HubDataFrame.h\"\n";

    if (gInterpreter->Declare(declaration.str().c_str()) == 0) {
        available = true;
        return true;
    }

    std::cerr << "[rarexsec] Unable to load librexsec_processing using the standard names, "
              << "the RAREXSEC_PROCESSING_LIBRARY override, or repository build directories, "
              << "and compiling the inline fallback failed. Build the project (e.g. 'make "
              << "build-lib') or set RAREXSEC_PROCESSING_LIBRARY to the compiled library "
              << "path.\n";
    available = false;
    return false;
}

} // namespace

void hub_preview() {
    if (!ensure_processing_library_available()) {
        return;
    }

    const std::string hub_path = "snapshot_fhc_r1-3_nuepre.hub.root";
    const std::string beam = "numi-fhc";
    const std::string period = "run1";
    const std::string variation = "nominal";
    const std::string origin = ""; // leave empty to accept any origin (mc, data, etc.)
    const std::string stage = "";  // leave empty to accept any processing stage

    proc::HubDataFrame hub{hub_path};

    try {
        auto df = hub.query(beam, period, variation, origin, stage);

        std::cout << "Opened hub: " << hub_path << '\n';
        std::cout << "Beam: " << beam << ", period: " << period << ", variation: " << variation;
        if (!origin.empty()) {
            std::cout << ", origin: " << origin;
        }
        if (!stage.empty()) {
            std::cout << ", stage: " << stage;
        }
        std::cout << "\n\n";

        auto columns = df.GetColumnNames();
        std::cout << "Available columns (" << columns.size() << "):\n";
        for (const auto &name : columns) {
            std::cout << "  - " << name << '\n';
        }
        std::cout << '\n';

        auto entry_count = df.Count();
        std::cout << "Total entries matching selection: " << entry_count.GetValue() << "\n\n";

        const std::vector<std::string> dataset_candidates = {
            "run", "sub", "evt", "reco_neutrino_energy", "reco_neutrino_vertex_z"};
        std::vector<std::string> dataset_columns;
        for (const auto &name : dataset_candidates) {
            if (df.HasColumn(name)) {
                dataset_columns.push_back(name);
            } else {
                std::cout << "[info] Dataset column '" << name << "' not found in this hub\n";
            }
        }
        if (!dataset_columns.empty()) {
            std::cout << "Preview of dataset columns:\n";
            auto preview = df.Display(dataset_columns, 5);
            preview->Print();
            std::cout << '\n';
        } else {
            std::cout << "[warning] None of the example dataset columns are available to preview\n\n";
        }

        const std::vector<std::string> friend_candidates = {
            "event_uid", "w_nom", "base_sel", "is_mc", "sampvar_uid"};
        std::vector<std::string> friend_columns;
        for (const auto &name : friend_candidates) {
            if (df.HasColumn(name)) {
                friend_columns.push_back(name);
            } else {
                std::cout << "[info] Friend column '" << name << "' not found in this hub\n";
            }
        }
        if (!friend_columns.empty()) {
            std::cout << "Preview of friend metadata columns:\n";
            auto friend_preview = df.Display(friend_columns, 5);
            friend_preview->Print();
            std::cout << '\n';
        } else {
            std::cout << "[warning] No friend metadata columns were available\n\n";
        }

    } catch (const std::exception &ex) {
        std::cerr << "Failed to load hub selection: " << ex.what() << '\n';
    }
}
