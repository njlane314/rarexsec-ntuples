#include <rarexsec/SamplePipeline.h>

#include <rarexsec/LoggerUtils.h>

namespace proc {
namespace {

ROOT::RDF::RNode buildBaseDataFrame(const std::string &base_dir, const std::string &rel_path,
                                    EventProcessorStage &processor, SampleOrigin origin) {
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
            log::info("SamplePipeline::applyExclusionKeys", "[warning]", "missing exclusion key",
                      exclusion_key);
        }
    }
    return df;
}

}

SamplePipeline::SamplePipeline(const nlohmann::json &sample_json, const nlohmann::json &all_samples_json,
                               const std::string &base_dir, const VariableRegistry &var_reg,
                               EventProcessorStage &processor)
    : descriptor_{SampleDescriptor::fromJson(sample_json)},
      nominal_node_{this->makeDataFrame(base_dir, var_reg, processor, descriptor_.relative_path, all_samples_json)} {
    this->validateFiles(base_dir);
    if (descriptor_.origin == SampleOrigin::kMonteCarlo) {
        for (const auto &variation_def : descriptor_.variations) {
            variation_nodes_.emplace(variation_def.variation,
                                     this->makeDataFrame(base_dir, var_reg, processor, variation_def.relative_path,
                                                         all_samples_json));
        }
    }
}

void SamplePipeline::validateFiles(const std::string &base_dir) const {
    if (descriptor_.sample_key.str().empty()) {
        log::fatal("SamplePipeline::validateFiles", "empty sample key");
    }
    if (descriptor_.origin == SampleOrigin::kUnknown) {
        log::fatal("SamplePipeline::validateFiles", "unknown sample origin", descriptor_.sample_key.str());
    }
    if ((descriptor_.origin == SampleOrigin::kMonteCarlo || descriptor_.origin == SampleOrigin::kDirt) &&
        descriptor_.pot <= 0) {
        log::fatal("SamplePipeline::validateFiles", "invalid pot for", descriptor_.sample_key.str());
    }
    if (descriptor_.origin == SampleOrigin::kData && descriptor_.triggers <= 0) {
        log::fatal("SamplePipeline::validateFiles", "invalid triggers for", descriptor_.sample_key.str());
    }
    if (descriptor_.origin != SampleOrigin::kData && descriptor_.relative_path.empty()) {
        log::fatal("SamplePipeline::validateFiles", "missing path for", descriptor_.sample_key.str());
    }
    if (!descriptor_.relative_path.empty()) {
        auto p = std::filesystem::path(base_dir) / descriptor_.relative_path;
        if (!std::filesystem::exists(p)) {
            log::fatal("SamplePipeline::validateFiles", "missing file", p.string());
        }
    }
    for (const auto &variation_def : descriptor_.variations) {
        if (variation_def.variation == SampleVariation::kUnknown) {
            log::fatal("SamplePipeline::validateFiles", "invalid variation type for",
                       variation_def.sample_key.str());
        }
        auto vp = std::filesystem::path(base_dir) / variation_def.relative_path;
        if (!std::filesystem::exists(vp)) {
            log::fatal("SamplePipeline::validateFiles", "missing variation", variation_def.relative_path);
        }
    }
}

ROOT::RDF::RNode SamplePipeline::makeDataFrame(const std::string &base_dir, const VariableRegistry &var_reg,
                                               EventProcessorStage &processor, const std::string &rel_path,
                                               const nlohmann::json &all_samples_json) {
    auto df = buildBaseDataFrame(base_dir, rel_path, processor, descriptor_.origin);
    df = applyTruthFilters(df, descriptor_.truth_filter);
    df = applyExclusionKeys(df, descriptor_.truth_exclusions, all_samples_json);
    const auto requested_columns = var_reg.columnsFor(descriptor_.origin);
    if (!requested_columns.empty()) {
        df = ROOT::RDF::RNode(df.Cache(requested_columns));
    }
    return df;
}

}
