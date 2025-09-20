#ifndef RAREXSEC_MAIN_RUNNERPARSER_H
#define RAREXSEC_MAIN_RUNNERPARSER_H

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <rarexsec/RunConfigRegistry.h>

namespace rarexsec::cli {

struct CommandLineOptions {
    std::filesystem::path config_path;
    std::string beam;
    std::vector<std::string> periods;
    std::optional<std::string> selection;
    std::optional<std::filesystem::path> output;
};

inline std::string trimCopy(std::string_view text) {
    const auto begin = text.find_first_not_of(" \t\n\r");
    if (begin == std::string_view::npos) {
        return {};
    }
    const auto end = text.find_last_not_of(" \t\n\r");
    return std::string{text.substr(begin, end - begin + 1)};
}

inline std::string toLowerCopy(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return text;
}

inline std::string canonicaliseBeamName(std::string_view beam) {
    std::string trimmed = trimCopy(beam);
    std::transform(trimmed.begin(), trimmed.end(), trimmed.begin(), [](unsigned char ch) {
        if (ch == '_') {
            return '-';
        }
        return static_cast<char>(std::tolower(ch));
    });
    return trimCopy(trimmed);
}

inline std::optional<int> extractRunNumber(std::string_view token) {
    std::string value = trimCopy(token);
    if (value.empty()) {
        return std::nullopt;
    }

    std::string lower = toLowerCopy(value);
    if (lower.rfind("run", 0) == 0) {
        lower.erase(0, 3);
    } else if (!lower.empty() && lower.front() == 'r') {
        lower.erase(0, 1);
    }

    lower = trimCopy(lower);
    if (lower.empty()) {
        return std::nullopt;
    }

    std::string digits;
    digits.reserve(lower.size());
    for (unsigned char ch : lower) {
        if (std::isdigit(ch)) {
            digits.push_back(static_cast<char>(ch));
        } else if (std::isspace(ch) || ch == '_' || ch == '-') {
            continue;
        } else {
            return std::nullopt;
        }
    }

    if (digits.empty()) {
        return std::nullopt;
    }

    try {
        return std::stoi(digits);
    } catch (const std::exception &) {
        return std::nullopt;
    }
}

inline std::string canonicalisePeriodName(std::string_view period) {
    std::string trimmed = trimCopy(period);
    if (trimmed.empty()) {
        return {};
    }

    std::string lower = toLowerCopy(trimmed);
    if (lower == "all" || lower == "*") {
        return "all";
    }

    if (auto number = extractRunNumber(lower)) {
        return "run" + std::to_string(*number);
    }

    return lower;
}

inline std::string joinList(const std::vector<std::string> &items, std::string_view separator) {
    std::ostringstream os;
    for (std::size_t i = 0; i < items.size(); ++i) {
        if (i != 0) {
            os << separator;
        }
        os << items[i];
    }
    return os.str();
}

inline std::vector<std::string> parsePeriods(std::string_view csv) {
    std::vector<std::string> periods;
    if (csv.empty()) {
        return periods;
    }

    std::stringstream stream(std::string{csv});
    std::string entry;
    while (std::getline(stream, entry, ',')) {
        std::string trimmed = trimCopy(entry);
        if (trimmed.empty()) {
            continue;
        }

        std::string lowered = toLowerCopy(trimmed);
        const auto hyphen_pos = lowered.find('-');
        bool expanded_range = false;
        if (hyphen_pos != std::string::npos) {
            const std::string start_token = trimCopy(lowered.substr(0, hyphen_pos));
            const std::string end_token = trimCopy(lowered.substr(hyphen_pos + 1));
            const auto start_number = extractRunNumber(start_token);
            const auto end_number = extractRunNumber(end_token);
            if (start_number && end_number) {
                if (*end_number < *start_number) {
                    throw std::invalid_argument("Invalid run period range: " + trimmed);
                }
                for (int value = *start_number; value <= *end_number; ++value) {
                    std::string period = "run" + std::to_string(value);
                    if (std::find(periods.begin(), periods.end(), period) == periods.end()) {
                        periods.push_back(std::move(period));
                    }
                }
                expanded_range = true;
            }
        }

        if (expanded_range) {
            continue;
        }

        std::string canonical = canonicalisePeriodName(trimmed);
        if (canonical.empty()) {
            continue;
        }

        if (canonical == "all") {
            if (!periods.empty()) {
                throw std::invalid_argument("The special period 'all' cannot be combined with explicit periods.");
            }
            periods.push_back(std::move(canonical));
            break;
        }

        if (std::find(periods.begin(), periods.end(), canonical) == periods.end()) {
            periods.push_back(std::move(canonical));
        }
    }

    return periods;
}

inline bool isCanonicalRunDesignator(const std::string &period) {
    if (period == "all") {
        return true;
    }
    if (period.rfind("run", 0) != 0 || period.size() <= 3) {
        return false;
    }
    return std::all_of(period.begin() + 3, period.end(), [](unsigned char ch) {
        return std::isdigit(ch);
    });
}

inline bool canonicalPeriodLess(const std::string &lhs, const std::string &rhs) {
    const auto lhs_number = extractRunNumber(lhs);
    const auto rhs_number = extractRunNumber(rhs);
    if (lhs_number && rhs_number && *lhs_number != *rhs_number) {
        return *lhs_number < *rhs_number;
    }
    return lhs < rhs;
}

inline std::string resolveBeam(const proc::RunConfigRegistry &registry, const std::string &beam) {
    std::unordered_map<std::string, std::string> canonical_to_actual;
    for (const auto &[label, config] : registry.all()) {
        (void)label;
        const std::string canonical = canonicaliseBeamName(config.beamMode());
        if (!canonical.empty() && canonical_to_actual.find(canonical) == canonical_to_actual.end()) {
            canonical_to_actual.emplace(canonical, config.beamMode());
        }
    }

    if (canonical_to_actual.empty()) {
        throw std::invalid_argument("No beam configurations are available in the provided catalogue.");
    }

    const std::string canonical_input = canonicaliseBeamName(beam);
    const auto it = canonical_to_actual.find(canonical_input);
    if (it == canonical_to_actual.end()) {
        std::vector<std::string> available;
        available.reserve(canonical_to_actual.size());
        for (const auto &[key, _] : canonical_to_actual) {
            available.push_back(key);
        }
        std::sort(available.begin(), available.end());
        std::ostringstream os;
        os << "Unknown beam '" << beam << "'. Available beams: " << joinList(available, ", ");
        throw std::invalid_argument(os.str());
    }

    return it->second;
}

inline std::vector<std::string> resolvePeriods(const proc::RunConfigRegistry &registry, const std::string &beam,
                                               const std::vector<std::string> &requested) {
    std::unordered_map<std::string, std::string> canonical_to_actual;
    for (const auto &[label, config] : registry.all()) {
        (void)label;
        if (config.beamMode() != beam) {
            continue;
        }

        const std::string canonical = canonicalisePeriodName(config.runPeriod());
        if (!canonical.empty() && canonical_to_actual.find(canonical) == canonical_to_actual.end()) {
            canonical_to_actual.emplace(canonical, config.runPeriod());
        }
    }

    if (canonical_to_actual.empty()) {
        std::ostringstream os;
        os << "No run periods are configured for beam '" << beam << "'.";
        throw std::invalid_argument(os.str());
    }

    std::vector<std::string> available;
    available.reserve(canonical_to_actual.size());
    for (const auto &[canonical, _] : canonical_to_actual) {
        available.push_back(canonical);
    }
    std::sort(available.begin(), available.end(), canonicalPeriodLess);
    available.erase(std::unique(available.begin(), available.end()), available.end());

    if (requested.size() == 1 && canonicalisePeriodName(requested.front()) == "all") {
        std::vector<std::string> resolved;
        resolved.reserve(available.size());
        for (const auto &canonical : available) {
            resolved.push_back(canonical_to_actual.at(canonical));
        }
        return resolved;
    }

    std::vector<std::string> resolved;
    resolved.reserve(requested.size());
    for (const auto &period : requested) {
        const std::string canonical = canonicalisePeriodName(period);
        const auto it = canonical_to_actual.find(canonical);
        if (it == canonical_to_actual.end()) {
            std::ostringstream os;
            os << "Run period '" << period << "' is not configured for beam '" << beam
               << "'. Available periods: " << joinList(available, ", ");
            throw std::invalid_argument(os.str());
        }
        if (std::find(resolved.begin(), resolved.end(), it->second) == resolved.end()) {
            resolved.push_back(it->second);
        }
    }

    return resolved;
}

inline CommandLineOptions parseArguments(int argc, char **argv) {
    const std::string program = argc > 0 ? argv[0] : "snapshot";
    const std::string usage =
        "Usage: " + program +
        " <config.json> <beam:{numi-fhc|numi-rhc|bnb}> <periods> [additional-periods...] [selection] [output.root]";

    if (argc < 4) {
        throw std::invalid_argument(usage);
    }

    CommandLineOptions options;
    options.config_path = std::filesystem::path{argv[1]};
    options.beam = canonicaliseBeamName(argv[2]);
    if (options.beam.empty()) {
        throw std::invalid_argument("No beam argument provided\n" + usage);
    }

    options.periods = parsePeriods(argv[3]);

    if (options.periods.empty()) {
        throw std::invalid_argument("No valid periods provided");
    }

    int next_arg = 4;
    while (next_arg < argc) {
        std::string_view candidate{argv[next_arg]};
        std::vector<std::string> additional = parsePeriods(candidate);
        if (additional.empty() ||
            !std::all_of(additional.begin(), additional.end(), isCanonicalRunDesignator)) {
            break;
        }

        if (std::find(options.periods.begin(), options.periods.end(), "all") != options.periods.end()) {
            throw std::invalid_argument("The special period 'all' cannot be combined with explicit periods.");
        }

        for (auto &period : additional) {
            if (period == "all" && !options.periods.empty()) {
                throw std::invalid_argument("The special period 'all' cannot be combined with explicit periods.");
            }
            if (std::find(options.periods.begin(), options.periods.end(), period) == options.periods.end()) {
                options.periods.push_back(std::move(period));
            }
        }
        ++next_arg;
    }

    if (std::find(options.periods.begin(), options.periods.end(), "all") != options.periods.end() &&
        options.periods.size() > 1) {
        throw std::invalid_argument("The special period 'all' cannot be combined with explicit periods.");
    }

    if (next_arg < argc) {
        std::string_view selection{argv[next_arg]};
        if (!selection.empty()) {
            options.selection = std::string(selection);
        }
        ++next_arg;
    }

    if (next_arg < argc) {
        std::string_view output{argv[next_arg]};
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
        ++next_arg;
    }

    if (next_arg < argc) {
        throw std::invalid_argument(std::string{"Too many arguments provided\n"} + usage);
    }

    return options;
}

}

#endif
