#ifndef ANALYSIS_DATA_LOADER_H
#define ANALYSIS_DATA_LOADER_H

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <rarexsec/AnalysisKey.h>
#include <rarexsec/BeamPeriodConfigRegistry.h>
#include <rarexsec/ConfiguredSample.h>
#include <rarexsec/IEventProcessor.h>
#include <rarexsec/SelectionQuery.h>
#include <rarexsec/VariableRegistry.h>

namespace proc {

class AnalysisDataLoader {
  public:
    using SampleFrameMap = std::map<SampleKey, ConfiguredSample>;

    AnalysisDataLoader(const BeamPeriodConfigRegistry &run_config_registry,
                       VariableRegistry variable_registry,
                       std::string beam_mode, std::vector<std::string> periods, std::string ntuple_base_dir,
                       bool blind = true);

    SampleFrameMap &getSampleFrames() noexcept { return frames_; }
    double getTotalPot() const noexcept { return total_pot_; }
    long getTotalTriggers() const noexcept { return total_triggers_; }
    const std::string &getBeam() const noexcept { return beam_; }
    const std::vector<std::string> &getPeriods() const noexcept { return periods_; }
    const BeamPeriodConfig *getRunConfigForSample(const SampleKey &sk) const;

    void snapshot(const std::string &filter_expr, const std::string &output_file,
                  const std::vector<std::string> &columns = {}) const;
    void snapshot(const SelectionQuery &query, const std::string &output_file,
                  const std::vector<std::string> &columns = {}) const;

    void printAllBranches() const;

  private:
    const BeamPeriodConfigRegistry &run_registry_;
    VariableRegistry var_registry_;
    std::string ntuple_base_directory_;

    std::string beam_;
    std::vector<std::string> periods_;
    bool blind_;

    double total_pot_;
    long total_triggers_;

    SampleFrameMap frames_;
    std::vector<std::unique_ptr<IEventProcessor>> processors_;
    std::unordered_map<SampleKey, const BeamPeriodConfig *> run_config_cache_;

    void loadAll();
    void processRunConfig(const BeamPeriodConfig &rc);

    template <typename Head, typename... Tail>
    std::unique_ptr<IEventProcessor> chainEventProcessors(std::unique_ptr<Head> head, std::unique_ptr<Tail>... tail);

    void writeSnapshotMetadata(const std::string &output_file) const;
    void reorganiseSnapshotTrees(const std::string &output_file) const;
};

template <typename Head, typename... Tail>
std::unique_ptr<IEventProcessor> AnalysisDataLoader::chainEventProcessors(std::unique_ptr<Head> head,
                                                                          std::unique_ptr<Tail>... tail) {
    if constexpr (sizeof...(tail) == 0) {
        return head;
    } else {
        auto next = this->chainEventProcessors(std::move(tail)...);
        head->chainNextProcessor(std::move(next));
        return head;
    }
}

}

#endif
