#include <rarexsec/processing/SampleDefinition.h>

#include <rarexsec/utils/Logger.h>

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
            log::warn("SampleDefinition::applyExclusionKeys", "missing exclusion key", exclusion_key);
        }
    }
    return df;
}

}

SampleDefinition::SampleDefinition(const nlohmann::json &sample_json, const nlohmann::json &all_samples_json,
                                   const std::string &base_dir, const VariableRegistry &var_reg,
                                   IEventProcessor &processor)
    : sample_key_{sample_json.at("sample_key").get<std::string>()},
      sample_origin_{[&]() {
          auto ts = sample_json.at("sample_type").get<std::string>();
          return (ts == "mc"     ? SampleOrigin::kMonteCarlo
                  : ts == "data" ? SampleOrigin::kData
                  : ts == "ext"  ? SampleOrigin::kExternal
                  : ts == "dirt" ? SampleOrigin::kDirt
                                  : SampleOrigin::kUnknown);
      }()},
      rel_path_{sample_json.value("relative_path", "")},
      truth_filter_{sample_json.value("truth_filter", "")},
      truth_exclusions_{sample_json.value("exclusion_truth_filters", std::vector<std::string>{})},
      pot_{sample_json.value("pot", 0.0)},
      triggers_{sample_json.value("triggers", 0L)},
      dataset_id_{sample_json.value("dataset_id", std::string{})},
      nominal_node_{makeDataFrame(base_dir, var_reg, processor, rel_path_, all_samples_json)} {
    if (sample_json.contains("detector_variations")) {
        const auto &detvars = sample_json.at("detector_variations");
        variation_samples_.reserve(detvars.size());
        for (auto &dv : detvars) {
            VariationSample variation_sample;
            variation_sample.sample_key = SampleKey{dv.at("sample_key").get<std::string>()};

            std::string variation_type = dv.value("variation_type", std::string{});
            variation_sample.variation = convertDetVarType(variation_type);
            if (!variation_type.empty()) {
                variation_sample.variation_label = "detvar_" + variation_type;
            }

            variation_sample.dataset_id = dv.value("dataset_id", std::string{});
            variation_sample.relative_path = dv.value("relative_path", std::string{});
            variation_sample.pot = dv.value("pot", 0.0);
            variation_sample.triggers = dv.value("triggers", 0L);

            if (sample_origin_ == SampleOrigin::kMonteCarlo && !variation_sample.relative_path.empty()) {
                variation_sample.node = makeDataFrame(base_dir, var_reg, processor, variation_sample.relative_path,
                                                      all_samples_json);
            }

            variation_samples_.push_back(std::move(variation_sample));
        }
    }
    validateFiles(base_dir);
}

void SampleDefinition::validateFiles(const std::string &base_dir) const {
    if (sample_key_.str().empty()) {
        log::fatal("SampleDefinition::validateFiles", "empty sample key");
    }
    if (sample_origin_ == SampleOrigin::kUnknown) {
        log::fatal("SampleDefinition::validateFiles", "unknown sample origin", sample_key_.str());
    }
    if ((sample_origin_ == SampleOrigin::kMonteCarlo || sample_origin_ == SampleOrigin::kDirt) && pot_ <= 0) {
        log::fatal("SampleDefinition::validateFiles", "invalid pot for", sample_key_.str());
    }
    if (sample_origin_ == SampleOrigin::kData && triggers_ <= 0) {
        log::fatal("SampleDefinition::validateFiles", "invalid triggers for", sample_key_.str());
    }
    if (sample_origin_ != SampleOrigin::kData && rel_path_.empty()) {
        log::fatal("SampleDefinition::validateFiles", "missing path for", sample_key_.str());
    }
    if (!rel_path_.empty()) {
        auto p = std::filesystem::path(base_dir) / rel_path_;
        if (!std::filesystem::exists(p)) {
            log::fatal("SampleDefinition::validateFiles", "missing file", p.string());
        }
    }
    for (const auto &variation : variation_samples_) {
        if (variation.relative_path.empty()) {
            log::fatal("SampleDefinition::validateFiles", "missing variation path for",
                      variation.sample_key.str());
        }
        auto vp = std::filesystem::path(base_dir) / variation.relative_path;
        if (!std::filesystem::exists(vp)) {
            log::fatal("SampleDefinition::validateFiles", "missing variation", variation.relative_path);
        }
    }
}

SampleVariation SampleDefinition::convertDetVarType(const std::string &s) const {
    if (s == "cv") return SampleVariation::kCV;
    if (s == "lyatt") return SampleVariation::kLYAttenuation;
    if (s == "lydown") return SampleVariation::kLYDown;
    if (s == "lyray") return SampleVariation::kLYRayleigh;
    if (s == "recomb2") return SampleVariation::kRecomb2;
    if (s == "sce") return SampleVariation::kSCE;
    if (s == "wiremodx") return SampleVariation::kWireModX;
    if (s == "wiremodyz") return SampleVariation::kWireModYZ;
    if (s == "wiremodanglexz") return SampleVariation::kWireModAngleXZ;
    if (s == "wiremodangleyz") return SampleVariation::kWireModAngleYZ;
    log::fatal("SampleDefinition::convertDetVarType", "invalid variation type", s);
    return SampleVariation::kUnknown;
}

ROOT::RDF::RNode SampleDefinition::makeDataFrame(const std::string &base_dir, const VariableRegistry &var_reg,
                                                 IEventProcessor &processor, const std::string &rel_path,
                                                 const nlohmann::json &all_samples_json) {
    auto df = buildBaseDataFrame(base_dir, rel_path, processor, sample_origin_);
    (void)var_reg;
    df = applyTruthFilters(df, truth_filter_);
    df = applyExclusionKeys(df, truth_exclusions_, all_samples_json);
    return df;
}

}
