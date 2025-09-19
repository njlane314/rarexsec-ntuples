#ifndef RAREXSEC_PROCESSING_CONFIGURED_SAMPLE_H
#define RAREXSEC_PROCESSING_CONFIGURED_SAMPLE_H

#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "ROOT/RDataFrame.hxx"
#include "nlohmann/json.hpp"

#include <rarexsec/processing/AnalysisKey.h>
#include <rarexsec/processing/IEventProcessor.h>
#include <rarexsec/processing/SampleDefinition.h>
#include <rarexsec/processing/SampleTypes.h>
#include <rarexsec/processing/VariableRegistry.h>

namespace proc {

class ConfiguredSample {
  public:
    ConfiguredSample(const nlohmann::json &sample_json, const nlohmann::json &all_samples_json,
                     const std::string &base_dir, const VariableRegistry &var_reg, IEventProcessor &processor);

    ConfiguredSample(ConfiguredSample &&) noexcept = default;
    ConfiguredSample &operator=(ConfiguredSample &&) noexcept = default;
    ConfiguredSample(const ConfiguredSample &) = delete;
    ConfiguredSample &operator=(const ConfiguredSample &) = delete;

    const SampleKey &sampleKey() const noexcept { return definition_.sample_key; }
    SampleOrigin sampleOrigin() const noexcept { return definition_.origin; }
    const std::string &relativePath() const noexcept { return definition_.relative_path; }
    const std::string &datasetId() const noexcept { return definition_.dataset_id; }
    const std::string &stageName() const noexcept { return definition_.stage_name; }
    const std::string &truthFilter() const noexcept { return definition_.truth_filter; }
    const std::vector<std::string> &truthExclusions() const noexcept { return definition_.truth_exclusions; }
    double pot() const noexcept { return definition_.pot; }
    long triggers() const noexcept { return definition_.triggers; }
    const SampleDefinition &definition() const noexcept { return definition_; }
    const std::vector<SampleVariationDefinition> &variationDefinitions() const noexcept {
        return definition_.variations;
    }
    ROOT::RDF::RNode nominalNode() const { return nominal_node_; }
    const std::map<SampleVariation, ROOT::RDF::RNode> &variationNodes() const noexcept { return variation_nodes_; }

    void validateFiles(const std::string &base_dir) const;

  private:
    SampleDefinition definition_;
    ROOT::RDF::RNode nominal_node_;
    std::map<SampleVariation, ROOT::RDF::RNode> variation_nodes_;
    ROOT::RDF::RNode makeDataFrame(const std::string &base_dir, const VariableRegistry &var_reg,
                                   IEventProcessor &processor, const std::string &rel_path,
                                   const nlohmann::json &all_samples_json);
};

}

#endif
