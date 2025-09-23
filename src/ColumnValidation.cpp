#include <rarexsec/ColumnValidation.h>

#include <algorithm>
#include <sstream>
#include <unordered_set>

#include <rarexsec/LoggerUtils.h>

namespace proc {
namespace {

std::string joinColumnNames(const std::vector<std::string> &columns) {
    std::ostringstream oss;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i != 0) {
            oss << ", ";
        }
        oss << columns[i];
    }
    return oss.str();
}

} // namespace

std::vector<std::string> collectMissingColumns(const ROOT::RDF::RNode &df,
                                               const std::vector<std::string> &columns) {
    std::vector<std::string> missing;
    missing.reserve(columns.size());

    for (const auto &column : columns) {
        if (!df.HasColumn(column)) {
            missing.push_back(column);
        }
    }

    return missing;
}

void reportMissingColumns(const SampleKey &sample_key, const std::string &rel_path, SampleOrigin origin,
                          ColumnRequirement requirement, const std::vector<std::string> &missing_columns) {
    if (missing_columns.empty()) {
        return;
    }

    constexpr bool kReportOptionalMissingColumns = false;
    if (requirement == ColumnRequirement::kOptional && !kReportOptionalMissingColumns) {
        return;
    }

    std::vector<std::string> sorted_missing{missing_columns.begin(), missing_columns.end()};
    std::sort(sorted_missing.begin(), sorted_missing.end());

    const std::string &sample_name = sample_key.str();
    const std::string &identifier = sample_name.empty() ? rel_path : sample_name;

    if (requirement == ColumnRequirement::kRequired &&
        (origin == SampleOrigin::kMonteCarlo || origin == SampleOrigin::kDirt)) {
        log::fatal("ColumnValidation::reportMissingColumns", "Missing required columns for", identifier,
                  "(origin:", originToString(origin), "):", joinColumnNames(sorted_missing));
    }

    static std::unordered_set<std::string> reported_signatures;
    std::ostringstream signature_stream;
    signature_stream << identifier << '|' << originToString(origin) << '|' <<
        (requirement == ColumnRequirement::kRequired ? "required" : "optional") << '|' <<
        joinColumnNames(sorted_missing);
    const auto signature = signature_stream.str();
    if (!reported_signatures.insert(signature).second) {
        return;
    }

    const char *requirement_description =
        requirement == ColumnRequirement::kRequired ? "Skipping unavailable required columns for"
                                                    : "Optional columns not available for";

    log::info("ColumnValidation::reportMissingColumns", "[warning]", requirement_description, identifier,
              "(origin:", originToString(origin), "):", joinColumnNames(sorted_missing));
}

} // namespace proc
