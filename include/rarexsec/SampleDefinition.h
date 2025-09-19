#ifndef SAMPLE_DEFINITION_H
#define SAMPLE_DEFINITION_H

#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include <rarexsec/AnalysisKey.h>
#include <rarexsec/SampleTypes.h>

namespace proc {

struct SampleVariationDefinition {
    SampleKey sample_key;
    SampleVariation variation{SampleVariation::kUnknown};
    std::string variation_label;
    std::string dataset_id;
    std::string relative_path;
    std::string stage_name;
    double pot{0.0};
    long triggers{0};
};

struct SampleDefinition {
    SampleKey sample_key;
    SampleOrigin origin{SampleOrigin::kUnknown};
    std::string dataset_id;
    std::string relative_path;
    std::string stage_name;
    std::string truth_filter;
    std::vector<std::string> truth_exclusions;
    double pot{0.0};
    long triggers{0};
    std::vector<SampleVariationDefinition> variations;

    static SampleDefinition fromJson(const nlohmann::json &sample_json);
};

inline SampleDefinition SampleDefinition::fromJson(const nlohmann::json &sample_json) {
    SampleDefinition definition;
    definition.sample_key = SampleKey{sample_json.at("sample_key").get<std::string>()};

    if (sample_json.contains("sample_type") && sample_json.at("sample_type").is_string()) {
        definition.origin = originFromString(sample_json.at("sample_type").get<std::string>());
    }

    definition.dataset_id = sample_json.value("dataset_id", std::string{});
    definition.relative_path = sample_json.value("relative_path", std::string{});
    definition.stage_name = sample_json.value("stage_name", std::string{});
    definition.truth_filter = sample_json.value("truth_filter", std::string{});
    definition.truth_exclusions = sample_json.value("exclusion_truth_filters", std::vector<std::string>{});
    definition.pot = sample_json.value("pot", 0.0);
    definition.triggers = sample_json.value("triggers", 0L);

    if (sample_json.contains("detector_variations") && sample_json.at("detector_variations").is_array()) {
        const auto &detvars = sample_json.at("detector_variations");
        definition.variations.reserve(detvars.size());
        for (const auto &dv : detvars) {
            SampleVariationDefinition var_def;
            var_def.sample_key = SampleKey{dv.at("sample_key").get<std::string>()};
            var_def.variation_label = dv.value("variation_type", std::string{});
            var_def.variation = variationFromString(var_def.variation_label);
            var_def.dataset_id = dv.value("dataset_id", std::string{});
            var_def.relative_path = dv.value("relative_path", std::string{});
            var_def.stage_name = dv.value("stage_name", std::string{});
            var_def.pot = dv.value("pot", 0.0);
            var_def.triggers = dv.value("triggers", 0L);
            definition.variations.emplace_back(std::move(var_def));
        }
    }

    return definition;
}

} // namespace proc

#endif
