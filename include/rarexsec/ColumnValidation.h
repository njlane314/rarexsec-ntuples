#ifndef RAREXSEC_COLUMN_VALIDATION_H
#define RAREXSEC_COLUMN_VALIDATION_H

#include <string>
#include <vector>

#include "ROOT/RDataFrame.hxx"

#include <rarexsec/AnalysisKey.h>
#include <rarexsec/SampleTypes.h>

namespace proc {
namespace column_validation {

enum class ColumnRequirement { kRequired, kOptional };

std::vector<std::string> missingColumnsForPlan(const ROOT::RDF::RNode &node,
                                               const std::vector<std::string> &plan,
                                               ColumnRequirement requirement);

void reportMissingColumns(const SampleKey &sample_key, const std::string &rel_path, SampleOrigin origin,
                          ColumnRequirement requirement, const std::vector<std::string> &missing_columns);

}  // namespace column_validation
}  // namespace proc

#endif  // RAREXSEC_COLUMN_VALIDATION_H
