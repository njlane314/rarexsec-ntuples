#ifndef SAMPLE_PIPELINE_H
#define SAMPLE_PIPELINE_H

#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "ROOT/RDataFrame.hxx"
#include "nlohmann/json.hpp"

#include <rarexsec/AnalysisKey.h>
#include <rarexsec/EventProcessorStage.h>
#include <rarexsec/SampleDescriptor.h>
#include <rarexsec/SampleTypes.h>
#include <rarexsec/VariableRegistry.h>

namespace proc {

class SamplePipeline {
  public:
    SamplePipeline(const nlohmann::json &sample_json, const nlohmann::json &all_samples_json,
                   const std::string &base_dir, const VariableRegistry &var_reg, EventProcessorStage &processor);

    SamplePipeline(SamplePipeline &&) noexcept = default;
    SamplePipeline &operator=(SamplePipeline &&) noexcept = default;
    SamplePipeline(const SamplePipeline &) = delete;
    SamplePipeline &operator=(const SamplePipeline &) = delete;

    const SampleKey &sampleKey() const noexcept { return descriptor_.sample_key; }
    SampleOrigin sampleOrigin() const noexcept { return descriptor_.origin; }
    const std::string &relativePath() const noexcept { return descriptor_.relative_path; }
    const std::string &stageName() const noexcept { return descriptor_.stage_name; }

    const std::string &truthFilter() const noexcept { return descriptor_.truth_filter; }
    const std::vector<std::string> &truthExclusions() const noexcept { return descriptor_.truth_exclusions; }

    double pot() const noexcept { return descriptor_.pot; }
    long triggers() const noexcept { return descriptor_.triggers; }

    const SampleDescriptor &descriptor() const noexcept { return descriptor_; }
    const std::vector<VariationDescriptor> &variationDescriptors() const noexcept { return descriptor_.variations; }

    ROOT::RDF::RNode nominalNode() const { return nominal_node_; }
    const std::map<SampleVariation, ROOT::RDF::RNode> &variationNodes() const noexcept { return variation_nodes_; }

    void validateFiles(const std::string &base_dir) const;

  private:
    SampleDescriptor descriptor_;
    ROOT::RDF::RNode nominal_node_;
    std::map<SampleVariation, ROOT::RDF::RNode> variation_nodes_;
    ROOT::RDF::RNode makeDataFrame(const std::string &base_dir, const VariableRegistry &var_reg,
                                   EventProcessorStage &processor, const std::string &rel_path,
                                   const nlohmann::json &all_samples_json);
};

}

#endif
