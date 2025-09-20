#include <exception>
#include <iostream>
#include <string>

#include <rarexsec/LoggerUtils.h>
#include <rarexsec/SnapshotPipelineBuilder.h>
#include <rarexsec/RunConfigLoader.h>

#include "RunnerParser.h"

int main(int argc, char **argv) {
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
        proc::SnapshotPipelineBuilder builder(registry, proc::VariableRegistry{}, resolved_beam, resolved_periods,
                                              *base_dir);
        if (options.output) {
            const std::string output_file = options.output->string();
            builder.snapshot(options.selection.value_or(""), output_file);
            proc::log::info("main", "Snapshot written to", output_file);
            std::cout << "ROOT snapshot saved to: " << output_file << std::endl;
        } else {
            builder.printAllBranches();
        }
    } catch (const std::exception &e) {
        std::cerr << "Processing failed: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
