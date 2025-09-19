#ifndef ANALYSIS_DATA_LOADER_H
#define ANALYSIS_DATA_LOADER_H

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ROOT/RDataFrame.hxx"

#include <rarexsec/core/AnalysisKey.h>
#include <rarexsec/data/BlipProcessor.h>
#include <rarexsec/data/IEventProcessor.h>
#include <rarexsec/utils/Logger.h>
#include <rarexsec/data/MuonSelectionProcessor.h>
#include <rarexsec/data/NuMuCCSelectionProcessor.h>
#include <rarexsec/data/PreselectionProcessor.h>
#include <rarexsec/data/ReconstructionProcessor.h>
#include <rarexsec/data/RunConfigRegistry.h>
#include <rarexsec/data/SampleDefinition.h>
#include <rarexsec/core/SelectionQuery.h>
#include <rarexsec/data/TruthChannelProcessor.h>
#include <rarexsec/data/VariableRegistry.h>
#include <rarexsec/data/WeightProcessor.h>

namespace analysis {

class AnalysisDataLoader {
  public:
    using SampleFrameMap = std::map<SampleKey, SampleDefinition>;

    AnalysisDataLoader(const RunConfigRegistry &run_config_registry, VariableRegistry variable_registry,
                       const std::string &beam_mode, std::vector<std::string> periods,
                       const std::string &ntuple_base_dir, bool blind = true)
        : run_registry_(run_config_registry),
          var_registry_(std::move(variable_registry)),
          ntuple_base_directory_(ntuple_base_dir),
          beam_(beam_mode),
          periods_(std::move(periods)),
          blind_(blind),
          total_pot_(0.0),
          total_triggers_(0) {
        this->loadAll();
    }

    SampleFrameMap &getSampleFrames() noexcept { return frames_; }
    double getTotalPot() const noexcept { return total_pot_; }
    long getTotalTriggers() const noexcept { return total_triggers_; }
    const std::string &getBeam() const noexcept { return beam_; }
    const std::vector<std::string> &getPeriods() const noexcept { return periods_; }
    const RunConfig *getRunConfigForSample(const SampleKey &sk) const {
        auto it = run_config_cache_.find(sk);
        if (it != run_config_cache_.end()) {
            return it->second;
        }
        return nullptr;
    }

    void snapshot(const std::string &filter_expr, const std::string &output_file,
                  const std::vector<std::string> &columns = {}) const {
        bool first = true;
        ROOT::RDF::RSnapshotOptions opts;
        for (auto const &[key, sample] : frames_) {
            auto df = sample.nominal_node_;
            if (!filter_expr.empty()) {
                df = df.Filter(filter_expr);
            }
            opts.fMode = first ? "RECREATE" : "UPDATE";
            df.Snapshot(key.c_str(), output_file, columns, opts);
            first = false;
        }
    }

    void snapshot(const SelectionQuery &query, const std::string &output_file,
                  const std::vector<std::string> &columns = {}) const {
        this->snapshot(query.str(), output_file, columns);
    }

    void printAllBranches() {
        log::debug("AnalysisDataLoader::printAllBranches", "Available branches in loaded samples:");
        for (auto &[sample_key, sample_def] : frames_) {
            log::debug("AnalysisDataLoader::printAllBranches", "--- Sample:", sample_key.str(), "---");
            auto branches = sample_def.nominal_node_.GetColumnNames();
            for (const auto &branch : branches) {
                log::debug("AnalysisDataLoader::printAllBranches", "  - ", branch);
            }
        }
    }

  private:
    const RunConfigRegistry &run_registry_;
    VariableRegistry var_registry_;
    std::string ntuple_base_directory_;

    std::string beam_;
    std::vector<std::string> periods_;
    bool blind_;

    double total_pot_;
    long total_triggers_;

    SampleFrameMap frames_;
    std::vector<std::unique_ptr<IEventProcessor>> processors_;
    std::unordered_map<SampleKey, const RunConfig *> run_config_cache_;

    void loadAll() {
        const std::string ext_beam{"numi_ext"};

        // First pass: accumulate total POT and triggers for all run periods
        // (including external samples) so that every WeightProcessor sees the
        // same overall totals.
        std::vector<const RunConfig *> configs_to_process;
        for (auto &period : periods_) {
            const auto &rc = run_registry_.get(beam_, period);
            total_pot_ += rc.nominal_pot;
            total_triggers_ += rc.nominal_triggers;
            configs_to_process.push_back(&rc);

            auto key = ext_beam + ":" + period;
            if (run_registry_.all().count(key)) {
                const auto &ext_rc = run_registry_.get(ext_beam, period);
                total_pot_ += ext_rc.nominal_pot;
                total_triggers_ += ext_rc.nominal_triggers;
                configs_to_process.push_back(&ext_rc);
            }
        }

        // Second pass: build processing pipelines using the final totals so all
        // samples are scaled consistently.
        for (const RunConfig *rc : configs_to_process) {
            this->processRunConfig(*rc);
        }
    }

    void processRunConfig(const RunConfig &rc) {
        processors_.reserve(processors_.size() + rc.samples.size());
        // SampleFrameMap is a std::map, which does not support reserve.
        for (auto &sample_json : rc.samples) {
            if (sample_json.contains("active") && !sample_json.at("active").get<bool>()) {
                log::info("AnalysisDataLoader::processRunConfig",
                          "Skipping inactive sample: ", sample_json.at("sample_key").get<std::string>());
                continue;
            }

            auto pipeline = this->chainEventProcessors(
                std::make_unique<WeightProcessor>(sample_json, total_pot_, total_triggers_),
                std::make_unique<TruthChannelProcessor>(), std::make_unique<BlipProcessor>(),
                  std::make_unique<MuonSelectionProcessor>(),
                  std::make_unique<ReconstructionProcessor>(),
                  std::make_unique<PreselectionProcessor>(),
                  std::make_unique<NuMuCCSelectionProcessor>());
            processors_.push_back(std::move(pipeline));

            auto &proc = *processors_.back();

            SampleDefinition sample{sample_json, rc.samples, ntuple_base_directory_, var_registry_, proc};

            run_config_cache_.emplace(sample.sample_key_, &rc);

            frames_.emplace(sample.sample_key_, std::move(sample));
        }
    }

    template <typename Head, typename... Tail>
    std::unique_ptr<IEventProcessor> chainEventProcessors(std::unique_ptr<Head> head, std::unique_ptr<Tail>... tail) {
        if constexpr (sizeof...(tail) == 0) {
            return head;
        } else {
            auto next = this->chainEventProcessors(std::move(tail)...);
            head->chainNextProcessor(std::move(next));
            return head;
        }
    }
};

}

#endif
