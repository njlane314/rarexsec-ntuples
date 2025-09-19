#include <rarexsec/processing/AnalysisDataLoader.h>

#include <rarexsec/processing/BlipProcessor.h>
#include <rarexsec/processing/MuonSelectionProcessor.h>
#include <rarexsec/processing/NuMuCCSelectionProcessor.h>
#include <rarexsec/processing/PreselectionProcessor.h>
#include <rarexsec/processing/ReconstructionProcessor.h>
#include <rarexsec/processing/TruthChannelProcessor.h>
#include <rarexsec/processing/WeightProcessor.h>
#include <rarexsec/utils/Logger.h>

#include "TDirectory.h"
#include "TFile.h"
#include "TObjString.h"
#include "TTree.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <utility>

namespace {

std::string toLowerCopy(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

std::string makeVariationLabel(const proc::SampleDefinition::VariationSample &variation) {
    if (!variation.variation_label.empty()) {
        return variation.variation_label;
    }
    auto key = proc::variationToKey(variation.variation);
    if (!key.empty() && key != "Unknown") {
        return "detvar_" + toLowerCopy(key);
    }
    return "detvar_unknown";
}

struct SampleSummary {
    std::string tree_name;
    std::string family;
    std::string variation;
    double sample_pot{0.0};
    long sample_triggers{0L};
};

} // namespace

namespace proc {

AnalysisDataLoader::AnalysisDataLoader(const RunConfigRegistry &run_config_registry, VariableRegistry variable_registry,
                                       std::string beam_mode, std::vector<std::string> periods,
                                       std::string ntuple_base_dir, bool blind)
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

const RunConfig *AnalysisDataLoader::getRunConfigForSample(const SampleKey &sk) const {
    auto it = run_config_cache_.find(sk);
    if (it != run_config_cache_.end()) {
        return it->second;
    }
    return nullptr;
}

void AnalysisDataLoader::snapshot(const std::string &filter_expr, const std::string &output_file,
                                  const std::vector<std::string> &columns) const {
    ROOT::RDF::RSnapshotOptions opts;
    opts.fCompressionAlgorithm = ROOT::kLZ4;
    opts.fCompressionLevel = 4;
    opts.fAutoFlush = 30 * 1024 * 1024;

    bool first_tree = true;
    std::vector<SampleSummary> summaries;
    summaries.reserve(frames_.size());

    for (auto const &[key, sample] : frames_) {
        auto df = sample.nominalNode();
        if (!filter_expr.empty()) {
            df = df.Filter(filter_expr);
        }
        opts.fMode = first_tree ? "RECREATE" : "UPDATE";
        df.Snapshot(key.c_str(), output_file, columns, opts);
        first_tree = false;

        SampleSummary summary{key.str(),
                              key.str(),
                              "nominal",
                              sample.pot(),
                              sample.triggers()};
        summaries.push_back(summary);

        if (sample.sampleOrigin() == SampleOrigin::kMonteCarlo) {
            for (const auto &variation : sample.variationSamples()) {
                auto vdf = variation.node;
                if (!filter_expr.empty()) {
                    vdf = vdf.Filter(filter_expr);
                }
                opts.fMode = first_tree ? "RECREATE" : "UPDATE";
                vdf.Snapshot(variation.sample_key.str().c_str(), output_file, columns, opts);
                first_tree = false;

                SampleSummary variation_summary{variation.sample_key.str(),
                                                summary.tree_name,
                                                makeVariationLabel(variation),
                                                variation.pot,
                                                variation.triggers};
                summaries.push_back(std::move(variation_summary));
            }
        }
    }

    std::unique_ptr<TFile> file{TFile::Open(output_file.c_str(), "UPDATE")};
    if (!file || file->IsZombie()) {
        throw std::runtime_error("Failed to open snapshot file for metadata: " + output_file);
    }

    TDirectory *meta_dir = file->GetDirectory("meta");
    if (!meta_dir) {
        meta_dir = file->mkdir("meta");
    }
    if (!meta_dir) {
        throw std::runtime_error("Failed to create meta directory in snapshot file");
    }
    meta_dir->cd();

    if (const auto &catalog_hash = run_registry_.catalogHash(); catalog_hash && !catalog_hash->empty()) {
        TObjString catalog_hash_obj(catalog_hash->c_str());
        catalog_hash_obj.Write("catalog_hash", TObject::kOverwrite);
    }

    double total_pot = total_pot_;
    Long64_t total_triggers = static_cast<Long64_t>(total_triggers_);
    TTree totals_tree("totals", "Integrated totals");
    totals_tree.Branch("total_pot", &total_pot);
    totals_tree.Branch("total_triggers", &total_triggers);
    totals_tree.Fill();
    totals_tree.Write("", TObject::kOverwrite);

    TTree samples_tree("samples", "Per-sample summary");
    std::string tree_name;
    std::string family;
    std::string variation_label;
    double sample_pot_value = 0.0;
    Long64_t sample_triggers_value = 0;

    samples_tree.Branch("tree_name", &tree_name);
    samples_tree.Branch("family", &family);
    samples_tree.Branch("variation", &variation_label);
    samples_tree.Branch("sample_pot", &sample_pot_value);
    samples_tree.Branch("sample_triggers", &sample_triggers_value);

    for (const auto &summary : summaries) {
        tree_name = summary.tree_name;
        family = summary.family;
        variation_label = summary.variation;
        sample_pot_value = summary.sample_pot;
        sample_triggers_value = static_cast<Long64_t>(summary.sample_triggers);
        samples_tree.Fill();
    }

    samples_tree.Write("", TObject::kOverwrite);

    meta_dir->cd();
    meta_dir->Write();
    file->Close();
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
    std::vector<const RunConfig *> configs_to_process;
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

    for (const RunConfig *rc : configs_to_process) {
        processRunConfig(*rc);
    }
}

void AnalysisDataLoader::processRunConfig(const RunConfig &rc) {
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

        SampleDefinition sample{sample_json, rc.sampleConfigs(), ntuple_base_directory_, var_registry_, processor};
        const auto sample_key = sample.sampleKey();

        auto insert_result = frames_.emplace(sample_key, std::move(sample));
        run_config_cache_.emplace(sample_key, &rc);

        auto &stored_sample = insert_result.first->second;
        for (const auto &variation : stored_sample.variationSamples()) {
            run_config_cache_.emplace(variation.sample_key, &rc);
        }
    }
}

}
