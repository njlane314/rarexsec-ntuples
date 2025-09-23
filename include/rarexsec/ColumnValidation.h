#ifndef RAREXSEC_COLUMN_VALIDATION_H
#define RAREXSEC_COLUMN_VALIDATION_H

#include <string>
#include <vector>

#include "ROOT/RDataFrame.hxx"

#include <rarexsec/AnalysisKey.h>
#include <rarexsec/SampleTypes.h>

namespace proc {

enum class ColumnRequirement { kRequired, kOptional };

std::vector<std::string> collectMissingColumns(const ROOT::RDF::RNode &df,
                                               const std::vector<std::string> &columns);

void reportMissingColumns(const SampleKey &sample_key, const std::string &rel_path, SampleOrigin origin,
                          ColumnRequirement requirement, const std::vector<std::string> &missing_columns);

}

#endif
