#include <rarexsec/AnalysisDataLoader.h>

#include "Compression.h"
#include "TDirectory.h"
#include "TFile.h"
#include "TROOT.h"
#include "TTree.h"
#include "ROOT/RDataFrame.hxx"

#include <cctype>
#include <sstream>

#include <rarexsec/Logger.h>
#include <rarexsec/BlipProcessor.h>
#include <rarexsec/MuonSelectionProcessor.h>
#include <rarexsec/PreselectionProcessor.h>
#include <rarexsec/ReconstructionProcessor.h>
#include <rarexsec/SampleTypes.h>
#include <rarexsec/TruthChannelProcessor.h>
#include <rarexsec/WeightProcessor.h>

namespace {

std::string sanitiseComponent(std::string value) {
    std::string sanitised;
    sanitised.reserve(value.size());
    for (unsigned char ch : value) {
        if (std::isalnum(ch) || ch == '_' || ch == '-') {
            sanitised.push_back(static_cast<char>(ch));
        } else {
            sanitised.push_back('_');
        }
    }
    if (sanitised.empty()) {
        sanitised = "unnamed";
    }
    return sanitised;
}

std::vector<std::string> nominalDirectoryComponents(const proc::SampleKey &sample_key) {
    return {"samples", sanitiseComponent(sample_key.str()), "nominal"};
}

std::vector<std::string> variationDirectoryComponents(const proc::SampleKey &base_key,
                                                      const proc::SampleVariationDefinition &variation_def) {
    std::string label = variation_def.variation_label;
    if (label.empty()) {
        label = proc::variationToKey(variation_def.variation);
    }
    return {"samples", sanitiseComponent(base_key.str()), "variations", sanitiseComponent(label)};
}

std::string componentsToPath(const std::vector<std::string> &components) {
    std::ostringstream os;
    for (std::size_t i = 0; i < components.size(); ++i) {
        if (i != 0) {
            os << '/';
        }
        os << components[i];
    }
    return os.str();
}

std::string nominalTreePath(const proc::SampleKey &sample_key) {
    return componentsToPath(nominalDirectoryComponents(sample_key)) + "/events";
}

std::string variationTreePath(const proc::SampleKey &base_key,
                              const proc::SampleVariationDefinition &variation_def) {
    return componentsToPath(variationDirectoryComponents(base_key, variation_def)) + "/events";
}

} // namespace

namespace proc {

AnalysisDataLoader::AnalysisDataLoader(const BeamPeriodConfigRegistry &run_config_registry,
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

const BeamPeriodConfig *AnalysisDataLoader::getRunConfigForSample(const SampleKey &sk) const {
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
    opts.fCompressionAlgorithm = ROOT::kLZ4;
    opts.fCompressionLevel = 4;
    opts.fAutoFlush = 30 * 1024 * 1024;

    auto snapshot_tree = [&](ROOT::RDF::RNode df, const std::string &tree_name) {
        if (!filter_expr.empty()) {
            df = df.Filter(filter_expr);
        }
        opts.fMode = first ? "RECREATE" : "UPDATE";
        df.Snapshot(tree_name, output_file, columns, opts);
        first = false;
    };

    for (auto const &[key, sample] : frames_) {
        snapshot_tree(sample.nominalNode(), key.str());
        for (const auto &variation_def : sample.variationDefinitions()) {
            const auto &variation_nodes = sample.variationNodes();
            auto it = variation_nodes.find(variation_def.variation);
            if (it == variation_nodes.end()) {
                continue;
            }
            snapshot_tree(it->second, variation_def.sample_key.str());
        }
    }

    if (first) {
        log::warn("AnalysisDataLoader::snapshot", "No samples were written to", output_file);
        return;
    }

    reorganiseSnapshotTrees(output_file);
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
    std::vector<const BeamPeriodConfig *> configs_to_process;
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

    for (const BeamPeriodConfig *rc : configs_to_process) {
        processRunConfig(*rc);
    }
}

void AnalysisDataLoader::processRunConfig(const BeamPeriodConfig &rc) {
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
            std::make_unique<PreselectionProcessor>());
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

    TTree samples_tree("samples", "Per-sample exposure summary");
    std::string tree_path;
    std::string sample_key_value;
    std::string beam;
    std::string run_period;
    std::string dataset_id;
    std::string relative_path;
    std::string variation_label;
    std::string stage_name;
    std::string origin_label;
    double sample_pot;
    long sample_triggers;

    samples_tree.Branch("tree_path", &tree_path);
    samples_tree.Branch("sample_key", &sample_key_value);
    samples_tree.Branch("beam", &beam);
    samples_tree.Branch("run_period", &run_period);
    samples_tree.Branch("dataset_id", &dataset_id);
    samples_tree.Branch("relative_path", &relative_path);
    samples_tree.Branch("variation", &variation_label);
    samples_tree.Branch("stage_name", &stage_name);
    samples_tree.Branch("origin", &origin_label);
    samples_tree.Branch("sample_pot", &sample_pot);
    samples_tree.Branch("sample_triggers", &sample_triggers);

    for (auto const &[key, sample] : frames_) {
        const auto *rc = getRunConfigForSample(key);
        beam = rc ? rc->beamMode() : std::string{};
        run_period = rc ? rc->runPeriod() : std::string{};

        tree_path = nominalTreePath(key);
        sample_key_value = key.str();
        dataset_id = sample.datasetId();
        relative_path = sample.relativePath();
        variation_label = "nominal";
        stage_name = sample.stageName();
        origin_label = originToString(sample.sampleOrigin());
        sample_pot = sample.pot();
        sample_triggers = sample.triggers();
        samples_tree.Fill();

        for (const auto &variation_def : sample.variationDefinitions()) {
            tree_path = variationTreePath(key, variation_def);
            sample_key_value = variation_def.sample_key.str();
            dataset_id = variation_def.dataset_id;
            relative_path = variation_def.relative_path;
            variation_label = variation_def.variation_label.empty() ? variationToKey(variation_def.variation)
                                                                    : variation_def.variation_label;
            stage_name = variation_def.stage_name.empty() ? sample.stageName() : variation_def.stage_name;
            origin_label = originToString(sample.sampleOrigin());
            sample_pot = variation_def.pot;
            sample_triggers = variation_def.triggers;

            const auto *variation_rc = getRunConfigForSample(variation_def.sample_key);
            if (variation_rc) {
                beam = variation_rc->beamMode();
                run_period = variation_rc->runPeriod();
            }

            samples_tree.Fill();
        }
    }

    samples_tree.Write("", TObject::kOverwrite);
    file->cd();
}

void AnalysisDataLoader::reorganiseSnapshotTrees(const std::string &output_file) const {
    std::unique_ptr<TFile> file{TFile::Open(output_file.c_str(), "UPDATE")};
    if (!file || file->IsZombie()) {
        log::fatal("AnalysisDataLoader::reorganiseSnapshotTrees", "Failed to open snapshot output", output_file);
    }

    const auto ensureDirectory = [&](const std::vector<std::string> &components) {
        TDirectory *current = file.get();
        for (const auto &component : components) {
            if (component.empty()) {
                continue;
            }
            TDirectory *next = current->GetDirectory(component.c_str());
            if (!next) {
                next = current->mkdir(component.c_str());
            }
            if (!next) {
                log::fatal("AnalysisDataLoader::reorganiseSnapshotTrees", "Failed to create directory component", component);
            }
            current = next;
        }
        return current;
    };

    const auto moveTree = [&](const std::string &tree_name, const std::vector<std::string> &components) {
        if (tree_name.empty()) {
            return;
        }
        TTree *tree = nullptr;
        file->GetObject(tree_name.c_str(), tree);
        if (!tree) {
            log::warn("AnalysisDataLoader::reorganiseSnapshotTrees", "Could not locate snapshot tree", tree_name);
            return;
        }

        TDirectory *target_dir = ensureDirectory(components);
        tree->SetDirectory(target_dir);
        tree->SetName("events");
        target_dir->cd();
        tree->Write("", TObject::kOverwrite);

        file->cd();
        file->Delete((tree_name + ";*").c_str());
    };

    for (auto const &[key, sample] : frames_) {
        moveTree(key.str(), nominalDirectoryComponents(key));
        for (const auto &variation_def : sample.variationDefinitions()) {
            moveTree(variation_def.sample_key.str(), variationDirectoryComponents(key, variation_def));
        }
    }

    file->cd();
    file->Write("", TObject::kOverwrite);
}

}
