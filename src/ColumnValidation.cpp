#include <rarexsec/ColumnValidation.h>

#include <algorithm>
#include <sstream>
#include <unordered_set>

#include <rarexsec/LoggerUtils.h>

namespace proc::column_validation {
namespace {

constexpr bool kReportOptionalMissingColumns = false;

bool shouldConsider(ColumnRequirement requirement) {
    return requirement == ColumnRequirement::kRequired || kReportOptionalMissingColumns;
}

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

}  // namespace

std::vector<std::string> missingColumnsForPlan(const ROOT::RDF::RNode &node,
                                               const std::vector<std::string> &plan,
                                               ColumnRequirement requirement) {
    if (!shouldConsider(requirement) || plan.empty()) {
        return {};
    }

    std::vector<std::string> missing;
    missing.reserve(plan.size());
    for (const auto &column : plan) {
        if (!node.HasColumn(column)) {
            missing.push_back(column);
        }
    }
    return missing;
}

void reportMissingColumns(const SampleKey &sample_key, const std::string &rel_path, SampleOrigin origin,
                          ColumnRequirement requirement, const std::vector<std::string> &missing_columns) {
    if (!shouldConsider(requirement) || missing_columns.empty()) {
        return;
    }

    std::vector<std::string> sorted_missing{missing_columns.begin(), missing_columns.end()};
    std::sort(sorted_missing.begin(), sorted_missing.end());

    const std::string &sample_name = sample_key.str();
    const std::string &identifier = sample_name.empty() ? rel_path : sample_name;

    if (requirement == ColumnRequirement::kRequired &&
        (origin == SampleOrigin::kMonteCarlo || origin == SampleOrigin::kDirt)) {
        log::fatal("SamplePipeline::makeDataFrame", "Missing required columns for", identifier, "(origin:",
                  originToString(origin), "):", joinColumnNames(sorted_missing));
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

    log::info("SamplePipeline::makeDataFrame", "[warning]", requirement_description, identifier, "(origin:",
              originToString(origin), "):", joinColumnNames(sorted_missing));
}

}  // namespace proc::column_validation
