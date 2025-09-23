// Run with: root -l -q 'tools/hub_preview.C'

#include "../include/rarexsec/HubDataFrame.h"

#include "TInterpreter.h"
#include "TSystem.h"

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDFHelpers.hxx"

#include <algorithm>
#include <cstdlib>
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

struct PreviewDefaults {
    static constexpr const char *hub_path = "snapshot_fhc_r1-3_nuepre.hub.root";
    static constexpr const char *beam = "numi-fhc";
    static constexpr const char *period = "run1";
    static constexpr const char *variation = "nominal";
    static constexpr const char *origin = "";
    static constexpr const char *stage = "";
};

std::string resolve_argument(const char *value, const char *default_value) {
    if (value != nullptr && value != default_value) {
        return value;
    }
    return default_value != nullptr ? default_value : "";
}

std::string resolve_hub_path(const char *value) {
    const bool provided = value != nullptr && value != PreviewDefaults::hub_path && value[0] != '\0';
    if (provided) {
        return value;
    }
    if (const char *env = std::getenv("HUB_PREVIEW_HUB")) {
        if (env[0] != '\0') {
            return env;
        }
    }
    if (value != nullptr && value[0] != '\0') {
        return value;
    }
    return PreviewDefaults::hub_path;
}

} // namespace

void hub_preview(const char *hub_path = PreviewDefaults::hub_path,
                 const char *beam = PreviewDefaults::beam,
                 const char *period = PreviewDefaults::period,
                 const char *variation = PreviewDefaults::variation,
                 const char *origin = PreviewDefaults::origin,
                 const char *stage = PreviewDefaults::stage) {
    if (!ensure_processing_library_available()) {
        return;
    }

    const std::string hub_path_value = resolve_hub_path(hub_path);
    const std::string beam_value = resolve_argument(beam, PreviewDefaults::beam);
    const std::string period_value = resolve_argument(period, PreviewDefaults::period);
    const std::string variation_value = resolve_argument(variation, PreviewDefaults::variation);
    const std::string origin_value = resolve_argument(origin, PreviewDefaults::origin);
    const std::string stage_value = resolve_argument(stage, PreviewDefaults::stage);

    if (!std::filesystem::exists(hub_path_value)) {
        std::cerr << "[rarexsec] Hub file '" << hub_path_value
                  << "' was not found. Provide a valid path or set HUB_PREVIEW_HUB.\n";
        return;
    }

    proc::HubDataFrame hub{hub_path_value};

    try {
        auto df = hub.query(beam_value, period_value, variation_value, origin_value, stage_value);

        std::cout << "Opened hub: " << hub_path_value << '\n';
        std::cout << "Beam: " << beam_value << ", period: " << period_value
                  << ", variation: " << variation_value;
        if (!origin_value.empty()) {
            std::cout << ", origin: " << origin_value;
        }
        if (!stage_value.empty()) {
            std::cout << ", stage: " << stage_value;
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
