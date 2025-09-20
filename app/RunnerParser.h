#ifndef RAREXSEC_MAIN_RUNNERPARSER_H
#define RAREXSEC_MAIN_RUNNERPARSER_H

#include <filesystem>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace rarexsec::cli {

struct CommandLineOptions {
    std::filesystem::path config_path;
    std::string beam;
    std::vector<std::string> periods;
    std::optional<std::string> selection;
    std::optional<std::filesystem::path> output;
};

inline std::vector<std::string> parsePeriods(std::string_view csv) {
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

inline CommandLineOptions parseArguments(int argc, char **argv) {
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

}

#endif
