#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <rarexsec/Logger.h>
#include <rarexsec/AnalysisDataLoader.h>
#include <rarexsec/BeamPeriodConfigLoader.h>

#include "RunnerParser.h"

int main(int argc, char **argv) {
    rarexsec::cli::CommandLineOptions options;
    try {
        options = rarexsec::cli::parseArguments(argc, argv);
    } catch (const std::invalid_argument &error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }

    proc::BeamPeriodConfigRegistry registry;
    try {
        proc::BeamPeriodConfigLoader::loadFromFile(options.config_path.string(), registry);
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
        proc::AnalysisDataLoader loader(registry, proc::VariableRegistry{}, options.beam, options.periods,
                                        *base_dir);
        if (options.output) {
            const std::string output_file = options.output->string();
            loader.snapshot(options.selection.value_or(""), output_file);
            proc::log::info("apps", "Snapshot written to", output_file);
            std::cout << "ROOT snapshot saved to: " << output_file << std::endl;
        } else {
            loader.printAllBranches();
        }
    } catch (const std::exception &e) {
        std::cerr << "Processing failed: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
