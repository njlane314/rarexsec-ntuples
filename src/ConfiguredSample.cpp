#include <rarexsec/ConfiguredSample.h>

#include <rarexsec/Logger.h>

namespace proc {
namespace {

ROOT::RDF::RNode buildBaseDataFrame(const std::string &base_dir, const std::string &rel_path,
                                    IEventProcessor &processor, SampleOrigin origin) {
    auto path = base_dir + "/" + rel_path;
    ROOT::RDataFrame df("nuselection/EventSelectionFilter", path);
    return processor.process(df, origin);
}

ROOT::RDF::RNode applyTruthFilters(ROOT::RDF::RNode df, const std::string &truth_filter) {
    return truth_filter.empty() ? df : df.Filter(truth_filter);
}

ROOT::RDF::RNode applyExclusionKeys(ROOT::RDF::RNode df, const std::vector<std::string> &truth_exclusions,
                                    const nlohmann::json &all_samples_json) {
    for (const auto &exclusion_key : truth_exclusions) {
        bool found_key = false;
        for (const auto &sample_json : all_samples_json) {
            if (sample_json.at("sample_key").get<std::string>() == exclusion_key) {
                if (sample_json.contains("truth_filter")) {
                    auto filter_str = sample_json.at("truth_filter").get<std::string>();
                    df = df.Filter("!(" + filter_str + ")");
                    found_key = true;
                    break;
                }
            }
        }
        if (!found_key) {
            log::warn("ConfiguredSample::applyExclusionKeys", "missing exclusion key", exclusion_key);
        }
    }
    return df;
}

}

ConfiguredSample::ConfiguredSample(const nlohmann::json &sample_json, const nlohmann::json &all_samples_json,
                                   const std::string &base_dir, const VariableRegistry &var_reg,
                                   IEventProcessor &processor)
    : definition_{SampleDefinition::fromJson(sample_json)},
      nominal_node_{makeDataFrame(base_dir, var_reg, processor, definition_.relative_path, all_samples_json)} {
    validateFiles(base_dir);
    if (definition_.origin == SampleOrigin::kMonteCarlo) {
        for (const auto &variation_def : definition_.variations) {
            variation_nodes_.emplace(variation_def.variation,
                                     makeDataFrame(base_dir, var_reg, processor, variation_def.relative_path,
                                                   all_samples_json));
        }
    }
}

void ConfiguredSample::validateFiles(const std::string &base_dir) const {
    if (definition_.sample_key.str().empty()) {
        log::fatal("ConfiguredSample::validateFiles", "empty sample key");
    }
    if (definition_.origin == SampleOrigin::kUnknown) {
        log::fatal("ConfiguredSample::validateFiles", "unknown sample origin", definition_.sample_key.str());
    }
    if ((definition_.origin == SampleOrigin::kMonteCarlo || definition_.origin == SampleOrigin::kDirt) &&
        definition_.pot <= 0) {
        log::fatal("ConfiguredSample::validateFiles", "invalid pot for", definition_.sample_key.str());
    }
    if (definition_.origin == SampleOrigin::kData && definition_.triggers <= 0) {
        log::fatal("ConfiguredSample::validateFiles", "invalid triggers for", definition_.sample_key.str());
    }
    if (definition_.origin != SampleOrigin::kData && definition_.relative_path.empty()) {
        log::fatal("ConfiguredSample::validateFiles", "missing path for", definition_.sample_key.str());
    }
    if (!definition_.relative_path.empty()) {
        auto p = std::filesystem::path(base_dir) / definition_.relative_path;
        if (!std::filesystem::exists(p)) {
            log::fatal("ConfiguredSample::validateFiles", "missing file", p.string());
        }
    }
    for (const auto &variation_def : definition_.variations) {
        if (variation_def.variation == SampleVariation::kUnknown) {
            log::fatal("ConfiguredSample::validateFiles", "invalid variation type for",
                       variation_def.sample_key.str());
        }
        auto vp = std::filesystem::path(base_dir) / variation_def.relative_path;
        if (!std::filesystem::exists(vp)) {
            log::fatal("ConfiguredSample::validateFiles", "missing variation", variation_def.relative_path);
        }
    }
}

ROOT::RDF::RNode ConfiguredSample::makeDataFrame(const std::string &base_dir, const VariableRegistry &var_reg,
                                                 IEventProcessor &processor, const std::string &rel_path,
                                                 const nlohmann::json &all_samples_json) {
    auto df = buildBaseDataFrame(base_dir, rel_path, processor, definition_.origin);
    (void)var_reg;
    df = applyTruthFilters(df, definition_.truth_filter);
    df = applyExclusionKeys(df, definition_.truth_exclusions, all_samples_json);
    return df;
}

}
