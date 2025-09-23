// Run with: root -l -q 'tools/hub_preview.C'

#include "../include/rarexsec/HubDataFrame.h"

#include "TInterpreter.h"
#include "TSystem.h"

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDFHelpers.hxx"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

constexpr unsigned int kPreviewRowLimit = 5;

template <typename DataFrame>
void preview_columns(DataFrame &df, const std::vector<std::string> &candidates,
                     const std::string &label, unsigned int row_limit = kPreviewRowLimit) {
    std::vector<std::string> available_columns;
    available_columns.reserve(candidates.size());

    auto capitalised_label = label;
    if (!capitalised_label.empty()) {
        capitalised_label[0] = std::toupper(static_cast<unsigned char>(capitalised_label[0]));
    }

    for (const auto &name : candidates) {
        if (df.HasColumn(name)) {
            available_columns.push_back(name);
        } else {
            std::cout << "[info] " << capitalised_label << " column '" << name
                      << "' not found in this hub\n";
        }
    }

    if (!available_columns.empty()) {
        std::cout << "Preview of " << label << " columns:\n";
        auto preview = df.Display(available_columns, row_limit);
        preview->Print();
        std::cout << '\n';
    } else {
        std::cout << "[warning] No " << label << " columns were available to preview\n\n";
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

    std::filesystem::path current_file = std::filesystem::absolute(__FILE__);
    auto repo_root = current_file.parent_path().parent_path();
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

    std::cerr << "[rarexsec] Unable to load librexsec_processing and failed to compile the"
              << " inline fallback. Build the project (e.g. 'make build-lib') or set"
              << " RAREXSEC_PROCESSING_LIBRARY to the compiled library path.\n";
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
        preview_columns(df, dataset_candidates, "dataset");

        const std::vector<std::string> friend_candidates = {
            "event_uid", "w_nom", "base_sel", "is_mc", "sampvar_uid"};
        preview_columns(df, friend_candidates, "friend metadata");

    } catch (const std::exception &ex) {
        std::cerr << "Failed to load hub selection: " << ex.what() << '\n';
    }
}
