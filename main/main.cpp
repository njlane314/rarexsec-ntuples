#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sstream>
#include <vector>

#include <rarexsec/Logger.h>
#include <rarexsec/AnalysisDataLoader.h>
#include <rarexsec/BeamPeriodConfigurationLoader.h>

namespace {

struct CommandLineOptions {
    std::filesystem::path config_path;
    std::string beam;
    std::vector<std::string> periods;
    std::optional<std::string> selection;
    std::optional<std::filesystem::path> output;
};

std::vector<std::string> parsePeriods(std::string_view csv) {
    std::vector<std::string> periods;
    if (csv.empty()) {
        return periods;
    }

    std::stringstream stream(std::string{csv});
    std::string entry;
    while (std::getline(stream, entry, ',')) {
        if (!entry.empty()) {
            periods.emplace_back(entry);
        }
    }

    return periods;
}

CommandLineOptions parseArguments(int argc, char **argv) {
    const std::string program = argc > 0 ? argv[0] : "rarexsec-runner";
    const std::string usage = "Usage: " + program +
                              " <config.json> <beam> <periods> [selection] [output.root]";

    if (argc < 4) {
        throw std::invalid_argument(usage);
    }
    if (argc > 6) {
        throw std::invalid_argument(std::string{"Too many arguments provided\n"} + usage);
    }

    CommandLineOptions options;
    options.config_path = std::filesystem::path{argv[1]};
    options.beam = argv[2];
    options.periods = parsePeriods(argv[3]);

    if (options.periods.empty()) {
        throw std::invalid_argument("No valid periods provided");
    }

    if (argc > 4) {
        std::string_view selection{argv[4]};
        if (!selection.empty()) {
            options.selection = std::string(selection);
        }
    }

    if (argc > 5) {
        std::string_view output{argv[5]};
        if (!output.empty()) {
            namespace fs = std::filesystem;
            fs::path path{output};
            const fs::path original_path{path};
            try {
                if (path.is_relative()) {
                    path = fs::absolute(path);
                }
                path = path.lexically_normal();
            } catch (const std::exception &) {
                path = original_path;
            }
            options.output = path;
        }
    }

    return options;
}

} // namespace

int main(int argc, char **argv) {
    CommandLineOptions options;
    try {
        options = parseArguments(argc, argv);
    } catch (const std::invalid_argument &error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }

    proc::BeamPeriodConfigurationRegistry registry;
    try {
        proc::BeamPeriodConfigurationLoader::loadFromFile(options.config_path.string(), registry);
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
            proc::log::info("main", "Snapshot written to", output_file);
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
