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
            const std::string hub_suffix = ".hub.root";
            if (output_file.size() < hub_suffix.size() ||
                output_file.compare(output_file.size() - hub_suffix.size(), hub_suffix.size(), hub_suffix) != 0) {
                proc::log::info("snapshot-analysis", "[warning]",
                                "Hub outputs conventionally use the .hub.root suffix:", output_file);
            }

            builder.snapshot(options.selection.value_or(""), output_file, {});
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
