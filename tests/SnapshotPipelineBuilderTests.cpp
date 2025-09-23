#include <rarexsec/SnapshotPipelineBuilder.h>

#include <rarexsec/EventProcessorStage.h>
#include <rarexsec/SamplePipeline.h>
#include <rarexsec/SampleTypes.h>
#include <rarexsec/VariableRegistry.h>
#include <rarexsec/RunConfigRegistry.h>

#include "ROOT/RDataFrame.hxx"
#include "TFile.h"
#include "TTree.h"

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace proc {

class SnapshotPipelineBuilderTestHelper {
  public:
    struct ComboSummary {
        uint32_t sample_id;
        uint16_t variation_id;
        uint8_t origin_id;
        SampleOrigin origin_enum;
        std::string sample_key;
        std::string variation_label;
        std::string stage;
        bool is_nominal;
    };

    static void logSampleDistributions(const SnapshotPipelineBuilder &builder) {
        builder.logSampleDistributions();
    }

    static ProvenanceDicts buildProvenanceDicts(const SnapshotPipelineBuilder &builder) {
        return builder.buildProvenanceDicts();
    }

    static std::pair<std::vector<ROOT::RDF::RNode>, std::vector<ComboSummary>>
    prepareFriendNodes(const SnapshotPipelineBuilder &builder, ProvenanceDicts &dicts) {
        auto [nodes, combos] = builder.prepareFriendNodes(dicts);
        std::vector<ComboSummary> summaries;
        summaries.reserve(combos.size());
        for (const auto &combo : combos) {
            summaries.push_back(ComboSummary{combo.sid,   combo.vid,   combo.oid,         combo.origin_enum,
                                             combo.sk,    combo.vlab, combo.stage,       combo.is_nominal});
        }
        return {std::move(nodes), std::move(summaries)};
    }
};

} // namespace proc

namespace {

class IdentityStage : public proc::EventProcessorStage {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, proc::SampleOrigin) const override { return df; }
};

class ClogRedirect {
  public:
    explicit ClogRedirect(std::ostream &target) : target_(target), old_buf_(std::clog.rdbuf(target.rdbuf())) {}
    ~ClogRedirect() { std::clog.rdbuf(old_buf_); }

  private:
    std::ostream &target_;
    std::streambuf *old_buf_;
};

std::filesystem::path createTemporaryDirectory() {
    static std::atomic<unsigned long long> counter{0};
    auto base = std::filesystem::temp_directory_path() / "rarexsec_snapshot_tests";
    std::filesystem::create_directories(base);
    auto unique = base / ("case_" +
                          std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "_" +
                          std::to_string(counter++));
    std::filesystem::create_directories(unique);
    return unique;
}

std::string createSampleFile(const std::filesystem::path &directory) {
    auto file_path = directory / "sample.root";
    TFile file(file_path.c_str(), "RECREATE");
    file.mkdir("nuselection");
    file.cd("nuselection");

    TTree tree("EventSelectionFilter", "EventSelectionFilter");
    int run = 1;
    int sub = 2;
    int evt = 3;
    bool passes_preselection = true;
    double base_event_weight = 1.0;
    tree.Branch("run", &run);
    tree.Branch("sub", &sub);
    tree.Branch("evt", &evt);
    tree.Branch("passes_preselection", &passes_preselection);
    tree.Branch("base_event_weight", &base_event_weight);
    tree.Fill();
    tree.Write();
    file.Close();

    return file_path.filename().string();
}

nlohmann::json makeSampleJson(const std::string &relative_path) {
    return nlohmann::json{{"sample_key", "test_sample"},
                          {"sample_type", "data"},
                          {"relative_path", relative_path},
                          {"stage_name", "qa-stage"},
                          {"pot", 0.0},
                          {"triggers", 10}};
}

struct BuilderContext {
    proc::RunConfigRegistry registry;
    std::filesystem::path base_dir;
    proc::SnapshotPipelineBuilder builder;
    proc::SampleKey sample_key;
    std::string stage_name;
    std::string sample_label;

    BuilderContext()
        : registry(),
          base_dir(createTemporaryDirectory()),
          builder(registry, proc::VariableRegistry{}, "test-beam", {}, base_dir.string(), true) {
        const auto relative_path = createSampleFile(base_dir);
        IdentityStage stage;
        auto sample_json = makeSampleJson(relative_path);
        nlohmann::json all_samples = nlohmann::json::array({sample_json});
        proc::VariableRegistry registry_copy;
        registry_copy.setBeamMode("test-beam");
        proc::SamplePipeline pipeline(sample_json, all_samples, base_dir.string(), registry_copy, stage);
        sample_key = pipeline.sampleKey();
        sample_label = sample_key.str();
        stage_name = sample_json.at("stage_name").get<std::string>();
        builder.getSampleFrames().emplace(sample_key, std::move(pipeline));
    }
};

#define CHECK(condition)                                                                               \
    do {                                                                                                \
        if (!(condition)) {                                                                             \
            std::cerr << __FILE__ << ":" << __LINE__ << " CHECK(" #condition ") failed" << std::endl; \
            return false;                                                                               \
        }                                                                                               \
    } while (false)

bool testLogSampleDistributionsEmpty() {
    auto temp_dir = createTemporaryDirectory();
    proc::RunConfigRegistry registry;
    proc::SnapshotPipelineBuilder builder(registry, proc::VariableRegistry{}, "test-beam", {}, temp_dir.string(), true);

    std::ostringstream oss;
    {
        ClogRedirect redirect(oss);
        proc::SnapshotPipelineBuilderTestHelper::logSampleDistributions(builder);
    }
    CHECK(oss.str().find("No samples have been queued for processing.") != std::string::npos);
    return true;
}

bool testLogSampleDistributionsWithSample() {
    BuilderContext context;

    std::ostringstream oss;
    {
        ClogRedirect redirect(oss);
        proc::SnapshotPipelineBuilderTestHelper::logSampleDistributions(context.builder);
    }

    const std::string log = oss.str();
    CHECK(log.find("Sample distribution by origin:") != std::string::npos);
    CHECK(log.find("data (1 sample)") != std::string::npos);
    CHECK(log.find(context.stage_name + " (1 sample)") != std::string::npos);
    CHECK(log.find("<unmapped> (1 sample)") != std::string::npos);
    return true;
}

bool testPrepareFriendNodesNominal() {
    BuilderContext context;

    auto dicts = proc::SnapshotPipelineBuilderTestHelper::buildProvenanceDicts(context.builder);
    auto [nodes, combos] = proc::SnapshotPipelineBuilderTestHelper::prepareFriendNodes(context.builder, dicts);

    CHECK(nodes.size() == 1);
    CHECK(combos.size() == 1);

    const auto &summary = combos.front();
    CHECK(summary.sample_key == context.sample_label);
    CHECK(summary.is_nominal);
    CHECK(summary.variation_label == "nominal");
    CHECK(summary.stage == context.stage_name);
    CHECK(summary.origin_enum == proc::SampleOrigin::kData);
    CHECK(summary.origin_id == dicts.origin2id.at(proc::SampleOrigin::kData));
    CHECK(dicts.sample2id.size() == 1);
    CHECK(dicts.sample2id.at(context.sample_label) == summary.sample_id);
    CHECK(dicts.var2id.count("nominal") == 1);

    auto &node = nodes.front();
    CHECK(node.HasColumn("event_uid"));
    CHECK(node.HasColumn("w_nom"));
    CHECK(node.HasColumn("base_sel"));
    CHECK(node.HasColumn("sampvar_uid"));
    CHECK(node.HasColumn("is_mc"));

    return true;
}

#undef CHECK

} // namespace

int main() {
    bool success = true;
    success &= testLogSampleDistributionsEmpty();
    success &= testLogSampleDistributionsWithSample();
    success &= testPrepareFriendNodesNominal();
    return success ? 0 : 1;
}

