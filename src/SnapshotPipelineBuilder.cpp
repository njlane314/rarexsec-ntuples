#include <rarexsec/SnapshotPipelineBuilder.h>

#include "TDirectory.h"
#include "TFile.h"
#include "TROOT.h"
#include "TTree.h"
#include "TObject.h"
#include "ROOT/RDataFrame.hxx"

#include <cctype>
#include <sstream>
#include <unordered_set>

#include <rarexsec/BlipProcessor.h>
#include <rarexsec/LoggerUtils.h>
#include <rarexsec/MuonSelectionProcessor.h>
#include <rarexsec/PreselectionProcessor.h>
#include <rarexsec/ReconstructionProcessor.h>
#include <rarexsec/SampleTypes.h>
#include <rarexsec/TruthChannelProcessor.h>
#include <rarexsec/WeightProcessor.h>

namespace {

constexpr char kEventsTreeName[] = "events";

class ImplicitMTGuard {
  public:
    ImplicitMTGuard() : was_enabled_{ROOT::IsImplicitMTEnabled()} {
        if (was_enabled_) {
            ROOT::DisableImplicitMT();
        }
    }

    ImplicitMTGuard(const ImplicitMTGuard &) = delete;
    ImplicitMTGuard &operator=(const ImplicitMTGuard &) = delete;

    ~ImplicitMTGuard() {
        if (was_enabled_) {
            ROOT::EnableImplicitMT();
        }
    }

  private:
    bool was_enabled_;
};

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

std::string canonicaliseBeamName(const std::string &beam) {
    std::string trimmed = beam;
    const auto begin = trimmed.find_first_not_of(" \t\n\r");
    if (begin == std::string::npos) {
        return {};
    }
    const auto end = trimmed.find_last_not_of(" \t\n\r");
    trimmed = trimmed.substr(begin, end - begin + 1);

    for (char &ch : trimmed) {
        if (ch == '_') {
            ch = '-';
        } else {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
    }

    return trimmed;
}

std::vector<std::string> baseDirectoryComponents(const proc::SampleKey &sample_key, const std::string &beam,
                                                 const std::string &period, proc::SampleOrigin origin,
                                                 const std::string &stage_name) {
    std::vector<std::string> components;
    components.reserve(7);
    components.emplace_back("samples");
    if (!beam.empty()) {
        components.emplace_back(sanitiseComponent(beam));
    }
    if (!period.empty()) {
        components.emplace_back(sanitiseComponent(period));
    }
    components.emplace_back(sanitiseComponent(proc::originToString(origin)));
    if (!stage_name.empty()) {
        components.emplace_back(sanitiseComponent(stage_name));
    }
    components.emplace_back(sanitiseComponent(sample_key.str()));
    return components;
}

std::vector<std::string> nominalDirectoryComponents(const proc::SampleKey &sample_key, const std::string &beam,
                                                    const std::string &period, proc::SampleOrigin origin,
                                                    const std::string &stage_name) {
    auto components = baseDirectoryComponents(sample_key, beam, period, origin, stage_name);
    components.emplace_back("nominal");
    return components;
}

std::vector<std::string> variationDirectoryComponents(const proc::SampleKey &base_key,
                                                      const proc::VariationDescriptor &variation_def,
                                                      const std::string &beam, const std::string &period,
                                                      proc::SampleOrigin origin, const std::string &stage_name) {
    auto components = baseDirectoryComponents(base_key, beam, period, origin, stage_name);
    components.emplace_back("variations");
    std::string label = variation_def.variation_label;
    if (label.empty()) {
        label = proc::variationToKey(variation_def.variation);
    }
    components.emplace_back(sanitiseComponent(label));
    return components;
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

std::string nominalTreePath(const proc::SampleKey &sample_key, const std::string &beam, const std::string &period,
                            proc::SampleOrigin origin, const std::string &stage_name) {
    return componentsToPath(nominalDirectoryComponents(sample_key, beam, period, origin, stage_name)) + "/events";
}

std::string variationTreePath(const proc::SampleKey &base_key, const proc::VariationDescriptor &variation_def,
                              const std::string &beam, const std::string &period, proc::SampleOrigin origin,
                              const std::string &stage_name) {
    return componentsToPath(variationDirectoryComponents(base_key, variation_def, beam, period, origin, stage_name)) +
           "/events";
}

}

namespace proc {

SnapshotPipelineBuilder::SnapshotPipelineBuilder(const RunConfigRegistry &run_config_registry,
                                                 VariableRegistry variable_registry, std::string beam_mode,
                                                 std::vector<std::string> periods, std::string ntuple_base_dir,
                                                 bool blind)
    : run_registry_(run_config_registry),
      var_registry_(std::move(variable_registry)),
      ntuple_base_directory_(std::move(ntuple_base_dir)),
      beam_(std::move(beam_mode)),
      periods_(std::move(periods)),
      blind_(blind),
      total_pot_(0.0),
      total_triggers_(0) {
    var_registry_.setBeamMode(beam_);
    this->loadAll();
}

const RunConfig *SnapshotPipelineBuilder::getRunConfigForSample(const SampleKey &sk) const {
    auto it = run_config_cache_.find(sk);
    if (it != run_config_cache_.end()) {
        return it->second;
    }
    return nullptr;
}

void SnapshotPipelineBuilder::snapshot(const std::string &filter_expr, const std::string &output_file,
                                       const std::vector<std::string> &columns) const {
    bool wrote_anything = false;
    ROOT::RDF::RSnapshotOptions opts;
    opts.fMode = "UPDATE";

    log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Starting snapshot over", frames_.size(),
              "samples to", output_file);
    if (filter_expr.empty()) {
        log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "No filter expression supplied");
    } else {
        log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Filter expression:", filter_expr);
    }

    const std::vector<std::string> *snapshot_columns = &columns;
    std::vector<std::string> deduplicated_columns;
    if (!columns.empty()) {
        std::unordered_set<std::string> seen_columns;
        seen_columns.reserve(columns.size());
        deduplicated_columns.reserve(columns.size());
        for (const auto &column : columns) {
            if (seen_columns.insert(column).second) {
                deduplicated_columns.push_back(column);
            } else {
                log::info("SnapshotPipelineBuilder::snapshot", "[warning]", "Duplicate snapshot column", column,
                          "will be ignored");
            }
        }
        if (deduplicated_columns.size() != columns.size()) {
            log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Removed",
                      columns.size() - deduplicated_columns.size(),
                      "duplicate column entries from snapshot request");
            snapshot_columns = &deduplicated_columns;
        }
    }

    if (snapshot_columns->empty()) {
        log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "No explicit column list provided");
    } else {
        std::ostringstream column_stream;
        for (std::size_t i = 0; i < snapshot_columns->size(); ++i) {
            if (i != 0) {
                column_stream << ", ";
            }
            column_stream << snapshot_columns->at(i);
        }
        log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Requested columns:", column_stream.str());
    }

    const auto initialiseOutputDirectories = [&](const std::vector<std::vector<std::string>> &directories) {
        if (directories.empty()) {
            log::info("SnapshotPipelineBuilder::snapshot", "[debug]",
                      "No directories requested during initialisation for", output_file);
            return;
        }

        log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Initialising", directories.size(),
                  "directory paths in", output_file);
        ImplicitMTGuard imt_guard;
        std::unique_ptr<TFile> file{TFile::Open(output_file.c_str(), "RECREATE")};
        if (!file || file->IsZombie()) {
            log::fatal("SnapshotPipelineBuilder::snapshot", "Failed to open snapshot output", output_file,
                       "with mode", "UPDATE");
        }

        for (const auto &components : directories) {
            if (components.empty()) {
                continue;
            }

            const std::string directory_path = componentsToPath(components);
            log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Ensuring directory path", directory_path);
            TDirectory *current = file.get();
            log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Starting at directory", current->GetPath());
            for (const auto &component : components) {
                if (component.empty()) {
                    continue;
                }

                log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Processing component", component,
                          "under", current->GetPath());
                TDirectory *next = current->GetDirectory(component.c_str());
                if (!next) {
                    if (TObject *existing = current->Get(component.c_str())) {
                        log::fatal("SnapshotPipelineBuilder::snapshot",
                                   "Existing object with requested name detected", existing->GetName(), "of type",
                                   existing->ClassName(), "under", current->GetPath());
                    }
                    log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Creating directory", component,
                              "under", current->GetPath());
                    next = current->mkdir(component.c_str());
                    if (!next) {
                        log::fatal("SnapshotPipelineBuilder::snapshot", "Failed to create directory component",
                                   component, "in", output_file);
                    }
                } else {
                    log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Reusing existing directory",
                              component, "under", current->GetPath());
                }
                current = next;
                log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Now at directory", current->GetPath());
            }
        }

        file->Write("", TObject::kOverwrite);
    };

    std::vector<std::vector<std::string>> directories_to_create;
    directories_to_create.reserve(frames_.size());
    std::unordered_set<std::string> scheduled_directories;
    scheduled_directories.reserve(frames_.size());

    auto schedule_directory = [&](std::vector<std::string> components, std::string description) {
        const std::string directory_path = componentsToPath(components);
        if (scheduled_directories.insert(directory_path).second) {
            log::info("SnapshotPipelineBuilder::snapshot", "[debug]", std::move(description), directory_path);
            directories_to_create.push_back(std::move(components));
        } else {
            log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Skipping duplicate directory", directory_path);
        }
    };

    std::size_t total_trees = 0;
    for (const auto &[key, sample] : frames_) {
        const auto *rc = this->getRunConfigForSample(key);
        const std::string sample_beam = rc ? rc->beamMode() : std::string{};
        const std::string sample_period = rc ? rc->runPeriod() : std::string{};
        const std::string &sample_stage = sample.stageName();

        schedule_directory(nominalDirectoryComponents(key, sample_beam, sample_period, sample.sampleOrigin(),
                                                      sample_stage),
                           "Scheduled nominal directory");
        ++total_trees;

        const auto &variation_nodes = sample.variationNodes();
        for (const auto &variation_def : sample.variationDescriptors()) {
            if (!variation_nodes.count(variation_def.variation)) {
                continue;
            }

            const auto *variation_rc = this->getRunConfigForSample(variation_def.sample_key);
            const std::string variation_beam = variation_rc ? variation_rc->beamMode() : sample_beam;
            const std::string variation_period = variation_rc ? variation_rc->runPeriod() : sample_period;
            const std::string &variation_stage =
                variation_def.stage_name.empty() ? sample_stage : variation_def.stage_name;

            schedule_directory(variationDirectoryComponents(key, variation_def, variation_beam, variation_period,
                                                            sample.sampleOrigin(), variation_stage),
                               "Scheduled variation directory");
            ++total_trees;
        }
    }

    initialiseOutputDirectories(directories_to_create);

    std::size_t processed_trees = 0;
    if (total_trees > 0) {
        log::info("SnapshotPipelineBuilder::snapshot", "Preparing to write", total_trees, "trees to", output_file);
    }

    const auto relocateTreeToDirectory = [&](const std::string &directory_path) {
        if (directory_path.empty()) {
            return;
        }

        ImplicitMTGuard imt_guard;
        std::unique_ptr<TFile> file{TFile::Open(output_file.c_str(), "UPDATE")};
        if (!file || file->IsZombie()) {
            log::fatal("SnapshotPipelineBuilder::snapshot", "Failed to reopen snapshot output", output_file,
                       "with mode", "UPDATE");
        }

        TDirectory *target_dir = file->GetDirectory(directory_path.c_str());
        if (!target_dir) {
            log::fatal("SnapshotPipelineBuilder::snapshot", "Requested target directory", directory_path,
                       "is not available in", output_file);
        }

        TObject *object = file->Get(kEventsTreeName);
        auto *tree = dynamic_cast<TTree *>(object);
        if (!tree) {
            log::fatal("SnapshotPipelineBuilder::snapshot", "Expected tree", kEventsTreeName,
                       "not found in", output_file, "after snapshot");
        }

        target_dir->cd();
        tree->SetDirectory(target_dir);
        tree->Write("", TObject::kOverwrite);
        file->cd();
        file->Delete((std::string(kEventsTreeName) + ";*").c_str());
        delete tree;
        file->Write("", TObject::kOverwrite);
        log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Relocated tree",
                  directory_path + '/' + kEventsTreeName, "into", output_file);
    };

    auto snapshot_tree = [&](ROOT::RDF::RNode df, const std::string &directory_path) {
        if (!filter_expr.empty()) {
            log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Applying filter to",
                      directory_path + '/' + kEventsTreeName);
            df = df.Filter(filter_expr);
        }
        log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Writing tree",
                  directory_path + '/' + kEventsTreeName, "to", output_file);
        auto tree_opts = opts;
        df.Snapshot(kEventsTreeName, output_file, *snapshot_columns, tree_opts);
        relocateTreeToDirectory(directory_path);
        wrote_anything = true;
        ++processed_trees;
        log::info("SnapshotPipelineBuilder::snapshot", "[progress]", "Wrote", processed_trees, '/', total_trees,
                  "trees -", directory_path + '/' + kEventsTreeName);
    };

    for (auto const &[key, sample] : frames_) {
        const auto *rc = this->getRunConfigForSample(key);
        const std::string sample_beam = rc ? rc->beamMode() : std::string{};
        const std::string sample_period = rc ? rc->runPeriod() : std::string{};
        const std::string &sample_stage = sample.stageName();

        const auto nominal_directory_components =
            nominalDirectoryComponents(key, sample_beam, sample_period, sample.sampleOrigin(), sample_stage);
        const auto nominal_directory_path = componentsToPath(nominal_directory_components);
        snapshot_tree(sample.nominalNode(), nominal_directory_path);
        const auto &variation_nodes = sample.variationNodes();
        for (const auto &variation_def : sample.variationDescriptors()) {
            auto it = variation_nodes.find(variation_def.variation);
            if (it == variation_nodes.end()) {
                continue;
            }
            const auto *variation_rc = this->getRunConfigForSample(variation_def.sample_key);
            const std::string variation_beam = variation_rc ? variation_rc->beamMode() : sample_beam;
            const std::string variation_period = variation_rc ? variation_rc->runPeriod() : sample_period;
            const std::string &variation_stage =
                variation_def.stage_name.empty() ? sample_stage : variation_def.stage_name;

            const auto variation_directory_components =
                variationDirectoryComponents(key, variation_def, variation_beam, variation_period,
                                             sample.sampleOrigin(), variation_stage);
            const auto variation_directory_path = componentsToPath(variation_directory_components);
            snapshot_tree(it->second, variation_directory_path);
        }
    }

    if (!wrote_anything) {
        log::info("SnapshotPipelineBuilder::snapshot", "[warning]", "No samples were written to", output_file);
        return;
    }

    this->writeSnapshotMetadata(output_file);
}

void SnapshotPipelineBuilder::snapshot(const FilterExpression &query, const std::string &output_file,
                                       const std::vector<std::string> &columns) const {
    this->snapshot(query.str(), output_file, columns);
}

void SnapshotPipelineBuilder::printAllBranches() const {
    log::info("SnapshotPipelineBuilder::printAllBranches", "[debug]",
              "Available branches in loaded samples");
    for (auto &[sample_key, sample_def] : frames_) {
        log::info("SnapshotPipelineBuilder::printAllBranches", "[debug]", "Sample", sample_key.str());
        auto branches = sample_def.nominalNode().GetColumnNames();
        for (const auto &branch : branches) {
            log::info("SnapshotPipelineBuilder::printAllBranches", "[debug]", branch);
        }
    }
}

void SnapshotPipelineBuilder::loadAll() {
    const std::string canonical_ext = canonicaliseBeamName("numi-ext");
    std::string ext_beam;
    for (const auto &[label, config] : run_registry_.all()) {
        (void)label;
        if (canonicaliseBeamName(config.beamMode()) == canonical_ext) {
            ext_beam = config.beamMode();
            break;
        }
    }

    std::vector<const RunConfig *> configs_to_process;
    for (auto &period : periods_) {
        const auto &rc = run_registry_.get(beam_, period);
        total_pot_ += rc.nominalPot();
        total_triggers_ += rc.nominalTriggers();
        configs_to_process.push_back(&rc);

        if (!ext_beam.empty()) {
            auto key = ext_beam + ":" + period;
            if (run_registry_.all().count(key)) {
                const auto &ext_rc = run_registry_.get(ext_beam, period);
                total_pot_ += ext_rc.nominalPot();
                total_triggers_ += ext_rc.nominalTriggers();
                configs_to_process.push_back(&ext_rc);
            }
        }
    }

    for (const RunConfig *rc : configs_to_process) {
        this->processRunConfig(*rc);
    }
}

void SnapshotPipelineBuilder::processRunConfig(const RunConfig &rc) {
    processors_.reserve(processors_.size() + rc.sampleConfigs().size());
    for (auto &sample_json : rc.sampleConfigs()) {
        if (sample_json.contains("active") && !sample_json.at("active").get<bool>()) {
            log::info("SnapshotPipelineBuilder::processRunConfig", "Skipping inactive sample",
                      sample_json.at("sample_key").get<std::string>());
            continue;
        }

        auto pipeline = this->chainProcessorStages(
            std::make_unique<WeightProcessor>(sample_json, total_pot_, total_triggers_),
            std::make_unique<TruthChannelProcessor>(), std::make_unique<BlipProcessor>(),
            std::make_unique<MuonSelectionProcessor>(), std::make_unique<ReconstructionProcessor>(),
            std::make_unique<PreselectionProcessor>());
        processors_.push_back(std::move(pipeline));

        auto &processor = *processors_.back();

        SamplePipeline sample{sample_json, rc.sampleConfigs(), ntuple_base_directory_, var_registry_, processor};
        const auto sample_key = sample.sampleKey();

        run_config_cache_.emplace(sample_key, &rc);
        for (const auto &variation_def : sample.variationDescriptors()) {
            run_config_cache_.emplace(variation_def.sample_key, &rc);
        }
        frames_.emplace(sample_key, std::move(sample));
    }
}

void SnapshotPipelineBuilder::writeSnapshotMetadata(const std::string &output_file) const {
    std::unique_ptr<TFile> file{TFile::Open(output_file.c_str(), "UPDATE")};
    if (!file || file->IsZombie()) {
        log::fatal("SnapshotPipelineBuilder::writeSnapshotMetadata", "Failed to open snapshot output", output_file);
    }

    TDirectory *meta_dir = file->GetDirectory("meta");
    if (!meta_dir) {
        meta_dir = file->mkdir("meta");
    }
    if (!meta_dir) {
        log::fatal("SnapshotPipelineBuilder::writeSnapshotMetadata", "Could not create meta directory");
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
    samples_tree.Branch("relative_path", &relative_path);
    samples_tree.Branch("variation", &variation_label);
    samples_tree.Branch("stage_name", &stage_name);
    samples_tree.Branch("origin", &origin_label);
    samples_tree.Branch("sample_pot", &sample_pot);
    samples_tree.Branch("sample_triggers", &sample_triggers);

    for (auto const &[key, sample] : frames_) {
        const auto *rc = this->getRunConfigForSample(key);
        const std::string sample_beam = rc ? rc->beamMode() : std::string{};
        const std::string sample_period = rc ? rc->runPeriod() : std::string{};
        const std::string &sample_stage = sample.stageName();

        beam = sample_beam;
        run_period = sample_period;
        tree_path = nominalTreePath(key, sample_beam, sample_period, sample.sampleOrigin(), sample_stage);
        sample_key_value = key.str();
        relative_path = sample.relativePath();
        variation_label = "nominal";
        stage_name = sample_stage;
        origin_label = originToString(sample.sampleOrigin());
        sample_pot = sample.pot();
        sample_triggers = sample.triggers();
        samples_tree.Fill();

        for (const auto &variation_def : sample.variationDescriptors()) {
            const auto *variation_rc = this->getRunConfigForSample(variation_def.sample_key);
            const std::string variation_beam = variation_rc ? variation_rc->beamMode() : sample_beam;
            const std::string variation_period = variation_rc ? variation_rc->runPeriod() : sample_period;
            const std::string &variation_stage =
                variation_def.stage_name.empty() ? sample_stage : variation_def.stage_name;

            tree_path = variationTreePath(key, variation_def, variation_beam, variation_period,
                                          sample.sampleOrigin(), variation_stage);
            sample_key_value = variation_def.sample_key.str();
            relative_path = variation_def.relative_path;
            variation_label = variation_def.variation_label.empty() ? variationToKey(variation_def.variation)
                                                                    : variation_def.variation_label;
            stage_name = variation_stage;
            origin_label = originToString(sample.sampleOrigin());
            sample_pot = variation_def.pot;
            sample_triggers = variation_def.triggers;
            beam = variation_beam;
            run_period = variation_period;

            samples_tree.Fill();
        }
    }

    samples_tree.Write("", TObject::kOverwrite);
    file->cd();
}

}
