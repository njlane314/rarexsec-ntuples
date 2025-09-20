#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include <rarexsec/SnapshotPipelineBuilder.h>
#include <rarexsec/RunConfigLoader.h>
#include <rarexsec/Logger.h>
#include <rarexsec/Selections.h>

#include "RunnerParser.h"

namespace {

const std::vector<std::string> &requestedTrainingPoolColumns() {
    static const std::vector<std::string> columns = {
        "run",
        "sub",
        "evt",
        "nominal_event_weight",
        "base_event_weight",
        "inclusive_strange_channels",
        "exclusive_strange_channels",
        "channel_definitions",
        "genie_int_mode",
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
        "is_vtx_in_image_w",
        "inference_score"};

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
            proc::log::info("rarexsec-training-pool", "[warning]", "Omitting column", column,
                            "because it is not available for every dataset");
        }
    }

    return available;
}

} // namespace

int main(int argc, char **argv) {
    rarexsec::cli::CommandLineOptions options;
    try {
        options = rarexsec::cli::parseArguments(argc, argv);
    } catch (const std::invalid_argument &error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }

    if (!options.output) {
        std::cerr << "An output file must be specified for the training pool." << std::endl;
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

    try {
        proc::SnapshotPipelineBuilder builder(registry, proc::VariableRegistry{}, options.beam, options.periods,
                                              *base_dir);

        const auto &frames = builder.getSampleFrames();
        auto columns = filterAvailableColumns(frames, requestedTrainingPoolColumns());
        if (columns.empty()) {
            std::cerr << "None of the requested training pool columns are available for the selected samples." << std::endl;
            return 1;
        }

        const std::string output_file = options.output->string();
        const proc::FilterExpression selection = options.selection
                                                   ? proc::FilterExpression{*options.selection}
                                                   : proc::muonNeutrinoChargedCurrentSelection();

        builder.snapshot(selection, output_file, columns);
        proc::log::info("rarexsec-training-pool", "Training pool snapshot written to", output_file);
        std::cout << "Training pool generated at: " << output_file << std::endl;
    } catch (const std::exception &e) {
        std::cerr << "Processing failed: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
