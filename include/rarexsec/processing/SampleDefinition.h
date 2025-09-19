#ifndef RAREXSEC_PROCESSING_SAMPLE_DEFINITION_H
#define RAREXSEC_PROCESSING_SAMPLE_DEFINITION_H

#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "ROOT/RDataFrame.hxx"
#include "nlohmann/json.hpp"

#include <rarexsec/processing/AnalysisKey.h>
#include <rarexsec/processing/IEventProcessor.h>
#include <rarexsec/processing/SampleTypes.h>
#include <rarexsec/processing/VariableRegistry.h>

namespace proc {

class SampleDefinition {
  public:
    SampleDefinition(const nlohmann::json &sample_json, const nlohmann::json &all_samples_json,
                     const std::string &base_dir, const VariableRegistry &var_reg, IEventProcessor &processor);

    SampleDefinition(SampleDefinition &&) noexcept = default;
    SampleDefinition &operator=(SampleDefinition &&) noexcept = default;
    SampleDefinition(const SampleDefinition &) = delete;
    SampleDefinition &operator=(const SampleDefinition &) = delete;

    const SampleKey &sampleKey() const noexcept { return sample_key_; }
    SampleOrigin sampleOrigin() const noexcept { return sample_origin_; }
    const std::string &relativePath() const noexcept { return rel_path_; }
    const std::string &truthFilter() const noexcept { return truth_filter_; }
    const std::vector<std::string> &truthExclusions() const noexcept { return truth_exclusions_; }
    double pot() const noexcept { return pot_; }
    long triggers() const noexcept { return triggers_; }
    ROOT::RDF::RNode nominalNode() const { return nominal_node_; }
    const std::map<SampleVariation, ROOT::RDF::RNode> &variationNodes() const noexcept { return variation_nodes_; }

    void validateFiles(const std::string &base_dir) const;

  private:
    SampleKey sample_key_;
    SampleOrigin sample_origin_;
    std::string rel_path_;
    std::string truth_filter_;
    std::vector<std::string> truth_exclusions_;
    double pot_;
    long triggers_;
    ROOT::RDF::RNode nominal_node_;
    std::map<SampleVariation, ROOT::RDF::RNode> variation_nodes_;
    std::map<SampleVariation, std::string> var_paths_;

    SampleVariation convertDetVarType(const std::string &s) const;
    ROOT::RDF::RNode makeDataFrame(const std::string &base_dir, const VariableRegistry &var_reg,
                                   IEventProcessor &processor, const std::string &rel_path,
                                   const nlohmann::json &all_samples_json);
};

}

#endif
