#include <rarexsec/processing/AnalysisDataLoader.h>

#include "Compression.h"
#include "TDirectory.h"
#include "TFile.h"
#include "TTree.h"
#include "ROOT/RDF/RSnapshotOptions.hxx"

#include <rarexsec/logging/Logger.h>
#include <rarexsec/processing/BlipProcessor.h>
#include <rarexsec/processing/MuonSelectionProcessor.h>
#include <rarexsec/processing/NuMuCCSelectionProcessor.h>
#include <rarexsec/processing/PreselectionProcessor.h>
#include <rarexsec/processing/ReconstructionProcessor.h>
#include <rarexsec/processing/SampleTypes.h>
#include <rarexsec/processing/TruthChannelProcessor.h>
#include <rarexsec/processing/WeightProcessor.h>

namespace proc {

namespace {

constexpr const char *kNominalTreeName = "nominal";
constexpr const char *kVariationDirectoryName = "variations";

std::string variationTreeName(const SampleVariationDefinition &variation_def) {
    if (!variation_def.variation_label.empty()) {
        return variation_def.variation_label;
    }
    return variation_def.sample_key.str();
}

std::string nominalTreePath(const SampleKey &sample_key) {
    return sample_key.str() + "/" + kNominalTreeName;
}

std::string variationTreePath(const SampleKey &sample_key, const SampleVariationDefinition &variation_def) {
    return sample_key.str() + "/" + kVariationDirectoryName + "/" + variationTreeName(variation_def);
}

} // namespace

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
    if (frames_.empty()) {
        log::warn("AnalysisDataLoader::snapshot", "No samples were written to", output_file);
        return;
    }

    ROOT::RDF::RSnapshotOptions opts;
    opts.fCompressionAlgorithm = ROOT::kLZ4;
    opts.fCompressionLevel = 4;
    opts.fAutoFlush = 30 * 1024 * 1024;

    const auto file_closer = [](TFile *file) {
        if (!file) {
            return;
        }
        file->Write("", TObject::kOverwrite);
        file->Close();
        delete file;
    };
    std::unique_ptr<TFile, decltype(file_closer)> snapshot_file{TFile::Open(output_file.c_str(), "RECREATE"),
                                                                file_closer};
    if (!snapshot_file || snapshot_file->IsZombie()) {
        log::fatal("AnalysisDataLoader::snapshot", "Failed to open snapshot output", output_file);
    }

    bool wrote_any = false;

    const auto snapshot_tree = [&](ROOT::RDF::RNode df, TDirectory *directory, const std::string &tree_name) {
        if (!directory) {
            log::fatal("AnalysisDataLoader::snapshot", "Invalid directory while writing snapshot", tree_name);
        }
        auto node = filter_expr.empty() ? df : df.Filter(filter_expr);
        node.Snapshot(tree_name, directory, columns, opts);
        wrote_any = true;
    };

    for (auto const &[key, sample] : frames_) {
        TDirectory *sample_dir = snapshot_file->GetDirectory(key.str().c_str());
        if (!sample_dir) {
            sample_dir = snapshot_file->mkdir(key.str().c_str());
        }
        if (!sample_dir) {
            log::fatal("AnalysisDataLoader::snapshot", "Could not create directory for sample", key.str());
        }

        snapshot_tree(sample.nominalNode(), sample_dir, kNominalTreeName);

        TDirectory *variations_dir = nullptr;
        const auto &variation_nodes = sample.variationNodes();
        for (const auto &variation_def : sample.variationDefinitions()) {
            auto it = variation_nodes.find(variation_def.variation);
            if (it == variation_nodes.end()) {
                continue;
            }

            if (!variations_dir) {
                variations_dir = sample_dir->GetDirectory(kVariationDirectoryName);
                if (!variations_dir) {
                    variations_dir = sample_dir->mkdir(kVariationDirectoryName);
                }
                if (!variations_dir) {
                    log::fatal("AnalysisDataLoader::snapshot", "Could not create variation directory for", key.str());
                }
            }

            snapshot_tree(it->second, variations_dir, variationTreeName(variation_def));
        }
    }

    snapshot_file.reset();

    if (!wrote_any) {
        log::warn("AnalysisDataLoader::snapshot", "No samples were written to", output_file);
        return;
    }

    writeSnapshotMetadata(output_file);
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
        for (const auto &variation_def : sample.variationDefinitions()) {
            run_config_cache_.emplace(variation_def.sample_key, &rc);
        }
        frames_.emplace(sample_key, std::move(sample));
    }
}

void AnalysisDataLoader::writeSnapshotMetadata(const std::string &output_file) const {
    std::unique_ptr<TFile> file{TFile::Open(output_file.c_str(), "UPDATE")};
    if (!file || file->IsZombie()) {
        log::fatal("AnalysisDataLoader::writeSnapshotMetadata", "Failed to open snapshot output", output_file);
    }

    TDirectory *meta_dir = file->GetDirectory("meta");
    if (!meta_dir) {
        meta_dir = file->mkdir("meta");
    }
    if (!meta_dir) {
        log::fatal("AnalysisDataLoader::writeSnapshotMetadata", "Could not create meta directory");
    }
    meta_dir->cd();

    TTree totals_tree("totals", "Exposure totals");
    double total_pot = total_pot_;
    long total_triggers = total_triggers_;
    totals_tree.Branch("total_pot", &total_pot);
    totals_tree.Branch("total_triggers", &total_triggers);
    totals_tree.Fill();
    totals_tree.Write("", TObject::kOverwrite);

    TTree exposure_tree("exposures", "Per-tree POT and trigger counts");
    std::string tree_path;
    double sample_pot = 0.0;
    long sample_triggers = 0;

    exposure_tree.Branch("tree_path", &tree_path);
    exposure_tree.Branch("pot", &sample_pot);
    exposure_tree.Branch("triggers", &sample_triggers);

    for (auto const &[key, sample] : frames_) {
        tree_path = nominalTreePath(key);
        sample_pot = sample.pot();
        sample_triggers = sample.triggers();
        exposure_tree.Fill();

        for (const auto &variation_def : sample.variationDefinitions()) {
            tree_path = variationTreePath(key, variation_def);
            sample_pot = variation_def.pot;
            sample_triggers = variation_def.triggers;
            exposure_tree.Fill();
        }
    }

    exposure_tree.Write("", TObject::kOverwrite);
    file->cd();
}

}
