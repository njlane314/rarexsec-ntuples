#include <algorithm>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <rarexsec/processing/AnalysisDataLoader.h>
#include <rarexsec/processing/RunConfigLoader.h>
#include <rarexsec/utils/Logger.h>

int main(int argc, char **argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <config.json> <beam> <periods> [selection] [output.root]" << std::endl;
        return 1;
    }

    std::string config_path = argv[1];
    std::string beam = argv[2];
    std::string periods_arg = argv[3];
    if (argc > 6) {
        std::cerr << "Too many arguments provided" << std::endl;
        return 1;
    }

    std::string selection = argc > 4 ? argv[4] : std::string{};
    std::string output = argc > 5 ? argv[5] : std::string{};

    std::vector<std::string> periods;
    const std::string_view periods_view{periods_arg};
    if (!periods_view.empty()) {
        periods.reserve(static_cast<std::size_t>(
            std::count(periods_view.begin(), periods_view.end(), ',')) + 1);
    }

    std::size_t start = 0;
    while (start < periods_view.size()) {
        const auto separator = periods_view.find(',', start);
        const auto length = separator == std::string_view::npos
                                 ? periods_view.size() - start
                                 : separator - start;

        if (length > 0) {
            periods.emplace_back(periods_view.substr(start, length));
        }

        if (separator == std::string_view::npos) {
            break;
        }

        start = separator + 1;
    }
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

    const auto &base_dir = registry.baseDirectory();
    if (!base_dir || base_dir->empty()) {
        std::cerr << "No ntuple directory configured in the JSON." << std::endl;
        return 1;
    }

    try {
        proc::AnalysisDataLoader loader(registry, proc::VariableRegistry{}, beam, periods, *base_dir);
        if (!output.empty()) {
            std::string resolved_output = output;
            try {
                namespace fs = std::filesystem;
                fs::path path{output};
                if (path.is_relative()) {
                    path = fs::absolute(path);
                }
                resolved_output = path.lexically_normal().string();
            } catch (const std::exception &) {
                // If canonicalization fails we fall back to the user-provided path.
            }

            if (!selection.empty()) {
                loader.snapshot(selection, output);
            } else {
                loader.snapshot("", output);
            }
            proc::log::info("main", "Snapshot written to", resolved_output);
            std::cout << "ROOT snapshot saved to: " << resolved_output << std::endl;
        } else {
            loader.printAllBranches();
        }
    } catch (const std::exception &e) {
        std::cerr << "Processing failed: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
