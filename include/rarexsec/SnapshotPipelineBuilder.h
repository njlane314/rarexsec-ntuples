#ifndef SNAPSHOT_PIPELINE_BUILDER_H
#define SNAPSHOT_PIPELINE_BUILDER_H

#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <rarexsec/AnalysisKey.h>
#include <rarexsec/HubCatalog.h>
#include <rarexsec/RunConfigRegistry.h>
#include <rarexsec/EventProcessorStage.h>
#include <rarexsec/FilterExpression.h>
#include <rarexsec/SamplePipeline.h>
#include <rarexsec/SampleTypes.h>
#include <rarexsec/VariableRegistry.h>

namespace proc {

class FriendWriter;

struct ProvenanceDicts {
  std::unordered_map<std::string, uint32_t> sample2id;
  std::unordered_map<std::string, uint16_t> beam2id, period2id, stage2id, var2id;
  std::unordered_map<SampleOrigin, uint8_t> origin2id;
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

    struct Combo {
        uint32_t sid;
        uint16_t vid;
        uint16_t bid;
        uint16_t pid;
        uint16_t stg;
        uint8_t oid;
        SampleOrigin origin_enum;
        std::string sk;
        std::string vlab;
        std::string beam;
        std::string period;
        std::string stage;
        std::string origin_label;
        std::string dataset_path;
        std::string dataset_tree;
        double pot;
        long triggers;
        bool is_nominal;
    };

    struct SnapshotPlan {
        ProvenanceDicts dicts;
        std::vector<ROOT::RDF::RNode> nodes;
        std::vector<Combo> combos;
    };

    void loadAll();
    void processRunConfig(const RunConfig &rc);

    void snapshotToHub(const std::string &hub_path,
                       const std::vector<std::string> &friend_columns,
                       std::vector<ROOT::RDF::RNode> &nodes, const std::vector<Combo> &combos,
                       const ProvenanceDicts &dicts) const;

    void logSampleSummary() const;
    SnapshotPlan buildSnapshotPlan() const;

    /**
     * Collect metadata entries for a single dataframe node and write its friend tree.
     *
     * Thread-safety: The caller must ensure that the provided FriendWriter supports
     * concurrent invocations of writeFriend. The hub directory, friend column list
     * and friend tree name must outlive the asynchronous task that executes this
     * helper.
     */
    std::vector<HubEntry> collectHubEntriesForNode(ROOT::RDF::RNode node,
                                                   const Combo &combo,
                                                   FriendWriter &writer,
                                                   const std::filesystem::path &hub_dir,
                                                   const std::vector<std::string> &friend_columns,
                                                   const std::string &friend_tree_name) const;
};

}

#endif
