// Run with: root -l -q 'tools/hub_preview.C'

#include "../include/rarexsec/HubDataFrame.h"

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDFHelpers.hxx"
#include "TSystem.h"

#include <iostream>
#include <string>
#include <vector>

namespace {

bool load_processing_library() {
    static bool attempted = false;
    static bool loaded = false;
    if (attempted) {
        return loaded;
    }
    attempted = true;

    auto try_load = [](const std::string &candidate) {
        return !candidate.empty() && gSystem->Load(candidate.c_str()) >= 0;
    };

    const std::vector<std::string> base_names = {
        "librarexsec_processing",
        "librarexsec_processing.so",
        "librarexsec_processing.dylib"
    };

    for (const auto &name : base_names) {
        if (try_load(name)) {
            loaded = true;
            return true;
        }
    }

    if (const char *override_path = gSystem->Getenv("RAREXSEC_PROCESSING_LIBRARY")) {
        if (try_load(override_path)) {
            loaded = true;
            return true;
        }
    }

    const std::string script_dir = gSystem->DirName(__FILE__);
    const std::vector<std::string> relative_dirs = {
        ".",
        "..",
        "../build",
        "../build/src",
        "../build-apps/src",
        "../install/lib"
    };

    for (const auto &dir : relative_dirs) {
        std::string path = script_dir + "/" + dir;
        gSystem->AddDynamicPath(path.c_str());
    }

    for (const auto &name : base_names) {
        if (try_load(name)) {
            loaded = true;
            return true;
        }
    }

    std::cerr << "[error] Unable to load librexsec_processing. "
              << "Build the project (for example with 'make') or set RAREXSEC_PROCESSING_LIBRARY "
              << "to the full path of the compiled library." << std::endl;
    return false;
}

} // namespace

void hub_preview() {
    if (!load_processing_library()) {
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
