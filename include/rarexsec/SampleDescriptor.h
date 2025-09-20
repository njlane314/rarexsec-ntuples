#ifndef SAMPLE_DESCRIPTOR_H
#define SAMPLE_DESCRIPTOR_H

#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include <rarexsec/AnalysisKey.h>
#include <rarexsec/SampleTypes.h>

namespace proc {

struct VariationDescriptor {
    SampleKey sample_key;
    SampleVariation variation{SampleVariation::kUnknown};

    std::string variation_label;
    std::string relative_path;
    std::string stage_name;

    double pot{0.0};
    long triggers{0};
};

struct SampleDescriptor {
    SampleKey sample_key;
    SampleOrigin origin{SampleOrigin::kUnknown};

    std::string relative_path;
    std::string stage_name;
    std::string truth_filter;
    std::vector<std::string> truth_exclusions;

    double pot{0.0};
    long triggers{0};

    std::vector<VariationDescriptor> variations;

    static SampleDescriptor fromJson(const nlohmann::json &sample_json);
};

inline SampleDescriptor SampleDescriptor::fromJson(const nlohmann::json &sample_json) {
    SampleDescriptor descriptor;
    descriptor.sample_key = SampleKey{sample_json.at("sample_key").get<std::string>()};

    if (sample_json.contains("sample_type") && sample_json.at("sample_type").is_string()) {
        descriptor.origin = originFromString(sample_json.at("sample_type").get<std::string>());
    }

    descriptor.relative_path = sample_json.value("relative_path", std::string{});
    descriptor.stage_name = sample_json.value("stage_name", std::string{});
    descriptor.truth_filter = sample_json.value("truth_filter", std::string{});
    descriptor.truth_exclusions = sample_json.value("exclusion_truth_filters", std::vector<std::string>{});
    descriptor.pot = sample_json.value("pot", 0.0);
    descriptor.triggers = sample_json.value("triggers", 0L);

    if (sample_json.contains("detector_variations") && sample_json.at("detector_variations").is_array()) {
        const auto &detvars = sample_json.at("detector_variations");
        descriptor.variations.reserve(detvars.size());
        for (const auto &dv : detvars) {
            VariationDescriptor variation;
            variation.sample_key = SampleKey{dv.at("sample_key").get<std::string>()};
            variation.variation_label = dv.value("variation_type", std::string{});
            variation.variation = variationFromString(variation.variation_label);
            variation.relative_path = dv.value("relative_path", std::string{});
            variation.stage_name = dv.value("stage_name", std::string{});
            variation.pot = dv.value("pot", 0.0);
            variation.triggers = dv.value("triggers", 0L);
            descriptor.variations.emplace_back(std::move(variation));
        }
    }

    return descriptor;
}

}

#endif
