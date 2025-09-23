// Run with: root -l -q 'tools/hub_preview.C'

#include "../include/rarexsec/HubDataFrame.h"

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDFHelpers.hxx"

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

void hub_preview() {
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
