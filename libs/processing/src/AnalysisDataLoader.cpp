#include <rarexsec/processing/AnalysisDataLoader.h>

#include <rarexsec/logging/Logger.h>
#include <rarexsec/processing/BlipProcessor.h>
#include <rarexsec/processing/MuonSelectionProcessor.h>
#include <rarexsec/processing/NuMuCCSelectionProcessor.h>
#include <rarexsec/processing/PreselectionProcessor.h>
#include <rarexsec/processing/ReconstructionProcessor.h>
#include <rarexsec/processing/TruthChannelProcessor.h>
#include <rarexsec/processing/WeightProcessor.h>

namespace proc {

AnalysisDataLoader::AnalysisDataLoader(const BeamPeriodConfigurationRegistry &run_config_registry,
                                       VariableRegistry variable_registry, std::string beam_mode,
                                       std::vector<std::string> periods, std::string ntuple_base_dir, bool blind)
    : run_registry_(run_config_registry),
      var_registry_(std::move(variable_registry)),
      ntuple_base_directory_(std::move(ntuple_base_dir)),
      beam_(std::move(beam_mode)),
      periods_(std::move(periods)),
      blind_(blind),
      total_pot_(0.0),
      total_triggers_(0) {
    loadAll();
}

const BeamPeriodConfiguration *AnalysisDataLoader::getRunConfigForSample(const SampleKey &sk) const {
    auto it = run_config_cache_.find(sk);
    if (it != run_config_cache_.end()) {
        return it->second;
    }
    return nullptr;
}

void AnalysisDataLoader::snapshot(const std::string &filter_expr, const std::string &output_file,
                                  const std::vector<std::string> &columns) const {
    bool first = true;
    ROOT::RDF::RSnapshotOptions opts;
    for (auto const &[key, sample] : frames_) {
        auto df = sample.nominalNode();
        if (!filter_expr.empty()) {
            df = df.Filter(filter_expr);
        }
        opts.fMode = first ? "RECREATE" : "UPDATE";
        df.Snapshot(key.c_str(), output_file, columns, opts);
        first = false;
    }
}

void AnalysisDataLoader::snapshot(const SelectionQuery &query, const std::string &output_file,
                                  const std::vector<std::string> &columns) const {
    snapshot(query.str(), output_file, columns);
}

void AnalysisDataLoader::printAllBranches() const {
    log::debug("AnalysisDataLoader::printAllBranches", "Available branches in loaded samples");
    for (auto &[sample_key, sample_def] : frames_) {
        log::debug("AnalysisDataLoader::printAllBranches", "Sample", sample_key.str());
        auto branches = sample_def.nominalNode().GetColumnNames();
        for (const auto &branch : branches) {
            log::debug("AnalysisDataLoader::printAllBranches", branch);
        }
    }
}

void AnalysisDataLoader::loadAll() {
    const std::string ext_beam{"numi_ext"};
    std::vector<const BeamPeriodConfiguration *> configs_to_process;
    for (auto &period : periods_) {
        const auto &rc = run_registry_.get(beam_, period);
        total_pot_ += rc.nominalPot();
        total_triggers_ += rc.nominalTriggers();
        configs_to_process.push_back(&rc);

        auto key = ext_beam + ":" + period;
        if (run_registry_.all().count(key)) {
            const auto &ext_rc = run_registry_.get(ext_beam, period);
            total_pot_ += ext_rc.nominalPot();
            total_triggers_ += ext_rc.nominalTriggers();
            configs_to_process.push_back(&ext_rc);
        }
    }

    for (const BeamPeriodConfiguration *rc : configs_to_process) {
        processRunConfig(*rc);
    }
}

void AnalysisDataLoader::processRunConfig(const BeamPeriodConfiguration &rc) {
    processors_.reserve(processors_.size() + rc.sampleConfigs().size());
    for (auto &sample_json : rc.sampleConfigs()) {
        if (sample_json.contains("active") && !sample_json.at("active").get<bool>()) {
            log::info("AnalysisDataLoader::processRunConfig", "Skipping inactive sample",
                      sample_json.at("sample_key").get<std::string>());
            continue;
        }

        auto pipeline = chainEventProcessors(
            std::make_unique<WeightProcessor>(sample_json, total_pot_, total_triggers_),
            std::make_unique<TruthChannelProcessor>(), std::make_unique<BlipProcessor>(),
            std::make_unique<MuonSelectionProcessor>(), std::make_unique<ReconstructionProcessor>(),
            std::make_unique<PreselectionProcessor>(), std::make_unique<NuMuCCSelectionProcessor>());
        processors_.push_back(std::move(pipeline));

        auto &processor = *processors_.back();

        ConfiguredSample sample{sample_json, rc.sampleConfigs(), ntuple_base_directory_, var_registry_, processor};
        const auto sample_key = sample.sampleKey();

        run_config_cache_.emplace(sample_key, &rc);
        frames_.emplace(sample_key, std::move(sample));
    }
}

}
