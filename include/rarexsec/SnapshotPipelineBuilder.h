#ifndef SNAPSHOT_PIPELINE_BUILDER_H
#define SNAPSHOT_PIPELINE_BUILDER_H

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <rarexsec/AnalysisKey.h>
#include <rarexsec/RunConfigRegistry.h>
#include <rarexsec/EventProcessorStage.h>
#include <rarexsec/FilterExpression.h>
#include <rarexsec/SamplePipeline.h>
#include <rarexsec/VariableRegistry.h>

class TFile;

namespace proc {

struct ProvenanceDicts {
  std::unordered_map<std::string, uint32_t> sample2id;
  std::unordered_map<std::string, uint16_t> beam2id, period2id, stage2id, var2id;
  std::unordered_map<SampleOrigin, uint8_t> origin2id;
};

struct CutflowRow {
  uint32_t sample_id;
  uint16_t variation_id, beam_id, period_id, stage_id;
  uint8_t origin_id;
  unsigned long long n_total = 0ULL;
  unsigned long long n_base = 0ULL;

  std::string sample_key;
  std::string variation;
  std::string beam;
  std::string period;
  std::string stage;
  std::string origin;
};

class SnapshotPipelineBuilder {
  public:
    using SampleFrameMap = std::map<SampleKey, SamplePipeline>;

    SnapshotPipelineBuilder(const RunConfigRegistry &run_config_registry, VariableRegistry variable_registry,
                            std::string beam_mode, std::vector<std::string> periods, std::string ntuple_base_dir,
                            bool blind = true);

    SampleFrameMap &getSampleFrames() noexcept { return frames_; }

    double getTotalPot() const noexcept { return total_pot_; }
    long getTotalTriggers() const noexcept { return total_triggers_; }

    const std::string &getBeam() const noexcept { return beam_; }
    const std::vector<std::string> &getPeriods() const noexcept { return periods_; }
    const RunConfig *getRunConfigForSample(const SampleKey &sk) const;

    void snapshot(const std::string &filter_expr, const std::string &output_file,
                  const std::vector<std::string> &columns = {}) const;
    void snapshot(const FilterExpression &query, const std::string &output_file,
                  const std::vector<std::string> &columns = {}) const;

    void printAllBranches() const;

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
    std::vector<std::unique_ptr<EventProcessorStage>> processors_;
    std::unordered_map<SampleKey, const RunConfig *> run_config_cache_;

    void loadAll();
    void processRunConfig(const RunConfig &rc);

    void writeSnapshotMetadata(TFile &output_file,
                               const ProvenanceDicts &dicts,
                               const std::vector<CutflowRow> &cutflow) const;
};

}

#endif
