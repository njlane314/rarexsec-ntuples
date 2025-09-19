#include <exception>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <rarexsec/processing/AnalysisDataLoader.h>
#include <rarexsec/processing/RunConfigLoader.h>
#include <rarexsec/utils/Logger.h>

namespace {
std::vector<std::string> parsePeriods(const std::string &arg) {
    std::vector<std::string> periods;
    std::stringstream ss(arg);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (!token.empty()) {
            periods.push_back(token);
        }
    }
    return periods;
}
}

int main(int argc, char **argv) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0] << " <config.json> <beam> <periods> <ntuple_dir> [selection] [output.root]" << std::endl;
        return 1;
    }

    std::string config_path = argv[1];
    std::string beam = argv[2];
    std::string periods_arg = argv[3];
    std::string ntuple_dir = argv[4];
    std::string selection = argc > 5 ? argv[5] : std::string{};
    std::string output = argc > 6 ? argv[6] : std::string{};

    auto periods = parsePeriods(periods_arg);
    if (periods.empty()) {
        std::cerr << "No valid periods provided" << std::endl;
        return 1;
    }

    proc::RunConfigRegistry registry;
    try {
        proc::RunConfigLoader::loadFromFile(config_path, registry);
    } catch (const std::exception &e) {
        std::cerr << "Failed to load run configuration: " << e.what() << std::endl;
        return 1;
    }

    try {
        proc::AnalysisDataLoader loader(registry, proc::VariableRegistry{}, beam, periods, ntuple_dir);
        if (!output.empty()) {
            if (!selection.empty()) {
                loader.snapshot(selection, output);
            } else {
                loader.snapshot("", output);
            }
            proc::log::info("main", "Snapshot written to", output);
        } else {
            loader.printAllBranches();
        }
    } catch (const std::exception &e) {
        std::cerr << "Processing failed: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
