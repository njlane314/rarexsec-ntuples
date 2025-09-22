#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include <TFile.h>
#include <TH1.h>
#include <TROOT.h>
#include <TTree.h>

#include "ROOT/RDataFrame.hxx"

#include <rarexsec/LoggerUtils.h>
#include <rarexsec/SnapshotPipelineBuilder.h>
#include <rarexsec/RunConfigLoader.h>

#include "RunnerParser.h"

namespace {

const std::vector<std::string> &requestedSnapshotColumns() {
    static const std::vector<std::string> columns = {
        "run",
        "sub",
        "evt",
        "nominal_event_weight",
        "base_event_weight",
        "inclusive_strange_channel_category",
        "exclusive_strange_channel_category",
        "channel_definition_category",
        "interaction_mode_category",
        "reco_neutrino_vertex_x",
        "reco_neutrino_vertex_y",
        "reco_neutrino_vertex_z",
        "detector_image_u",
        "detector_image_v",
        "detector_image_w",
        "semantic_image_u",
        "semantic_image_v",
        "semantic_image_w",
        "event_detector_image_u",
        "event_detector_image_v",
        "event_detector_image_w",
        "event_semantic_image_u",
        "event_semantic_image_v",
        "event_semantic_image_w",
        "event_adc_u",
        "event_adc_v",
        "event_adc_w",
        "slice_semantic_counts_u",
        "slice_semantic_counts_v",
        "slice_semantic_counts_w",
        "event_semantic_counts_u",
        "event_semantic_counts_v",
        "event_semantic_counts_w",
        "is_vtx_in_image_u",
        "is_vtx_in_image_v",
        "is_vtx_in_image_w"};

    return columns;
}

std::vector<std::string> filterAvailableColumns(const proc::SnapshotPipelineBuilder::SampleFrameMap &frames,
                                                const std::vector<std::string> &requested) {
    std::vector<std::string> available;
    available.reserve(requested.size());

    for (const auto &column : requested) {
        bool present_everywhere = true;
        for (const auto &[sample_key, sample] : frames) {
            (void)sample_key;
            if (!sample.nominalNode().HasColumn(column)) {
                present_everywhere = false;
                break;
            }
            for (const auto &[variation, node] : sample.variationNodes()) {
                (void)variation;
                auto mutable_node = node;
                if (!mutable_node.HasColumn(column)) {
                    present_everywhere = false;
                    break;
                }
            }
            if (!present_everywhere) {
                break;
            }
        }

        if (present_everywhere) {
            available.push_back(column);
        } else {
            proc::log::info("snapshot-analysis", "[warning]", "Omitting column", column,
                            "because it is not available for every dataset");
        }
    }

    return available;
}

} // namespace

int main(int argc, char **argv) {
    ROOT::EnableImplicitMT();
    gROOT->SetBatch(kTRUE);
    TH1::AddDirectory(kFALSE);
    TFile::SetReadaheadSize(256 * 1024 * 1024);
    TTree::SetMaxTreeSize(1000000000000LL);

    rarexsec::cli::CommandLineOptions options;
    try {
        options = rarexsec::cli::parseArguments(argc, argv);
    } catch (const std::invalid_argument &error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }

    proc::RunConfigRegistry registry;
    try {
        proc::RunConfigLoader::loadFromFile(options.config_path.string(), registry);
    } catch (const std::exception &e) {
        std::cerr << "Failed to load run configuration: " << e.what() << std::endl;
        return 1;
    }

    const auto &base_dir = registry.baseDirectory();
    if (!base_dir || base_dir->empty()) {
        std::cerr << "No ntuple directory configured in the JSON." << std::endl;
        return 1;
    }

    std::string resolved_beam;
    std::vector<std::string> resolved_periods;
    try {
        resolved_beam = rarexsec::cli::resolveBeam(registry, options.beam);
        resolved_periods = rarexsec::cli::resolvePeriods(registry, resolved_beam, options.periods);
    } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    try {
        proc::log::info("snapshot-analysis", "Enabling ROOT implicit MT with the maximum available threads");
        if (!ROOT::IsImplicitMTEnabled()) {
            ROOT::EnableImplicitMT();
        }

        proc::SnapshotPipelineBuilder builder(registry, proc::VariableRegistry{}, resolved_beam, resolved_periods,
                                              *base_dir);
        if (options.output) {
            const std::string output_file = options.output->string();
            auto columns = filterAvailableColumns(builder.getSampleFrames(), requestedSnapshotColumns());
            if (columns.empty()) {
                std::cerr << "None of the requested snapshot columns are available for the selected samples."
                          << std::endl;
                return 1;
            }

            const std::string hub_suffix = ".hub.root";
            if (output_file.size() < hub_suffix.size() ||
                output_file.compare(output_file.size() - hub_suffix.size(), hub_suffix.size(), hub_suffix) != 0) {
                proc::log::info("snapshot-analysis", "[warning]",
                                "Hub outputs conventionally use the .hub.root suffix:", output_file);
            }

            builder.snapshot(options.selection.value_or(""), output_file, columns);
            proc::log::info("snapshot-analysis", "Hub snapshot written to", output_file);
            std::cout << "Hub snapshot saved to: " << output_file << std::endl;
        } else {
            builder.printAllBranches();
        }
    } catch (const std::exception &e) {
        std::cerr << "Processing failed: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
