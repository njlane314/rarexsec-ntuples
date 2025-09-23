#include <rarexsec/SnapshotPipelineBuilder.h>

#include "ROOT/RDataFrame.hxx"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <future>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <rarexsec/BlipProcessor.h>
#include <rarexsec/LoggerUtils.h>
#include <rarexsec/MuonSelectionProcessor.h>
#include <rarexsec/PreselectionProcessor.h>
#include <rarexsec/ProcessorPipeline.h>
#include <rarexsec/HubCatalog.h>
#include <rarexsec/FriendWriter.h>
#include <rarexsec/ReconstructionProcessor.h>
#include <rarexsec/SampleTypes.h>
#include <rarexsec/TruthChannelProcessor.h>
#include <rarexsec/WeightProcessor.h>

namespace {
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

template <typename MapT, typename KeyT, typename IdT>
static IdT intern(MapT &m, const KeyT &k) {
    auto it = m.find(k);
    if (it != m.end()) {
        return it->second;
    }
    IdT id = static_cast<IdT>(m.size());
    m.emplace(k, id);
    return id;
}

static std::string variationLabelOrKey(const proc::VariationDescriptor &vd) {
    return vd.variation_label.empty() ? proc::variationToKey(vd.variation) : vd.variation_label;
}

constexpr const char *kInputTreeName = "nuselection/EventSelectionFilter";

static ROOT::RDF::RNode configureFriendNode(ROOT::RDF::RNode df, bool is_mc, uint64_t sampvar_uid) {
    if (df.HasColumn("run") && df.HasColumn("sub") && df.HasColumn("evt")) {
        df = df.Define("event_uid",
                       [](ULong64_t run, ULong64_t sub, ULong64_t evt) {
                           return (run << 42U) | (sub << 21U) | evt;
                       },
                       {"run", "sub", "evt"});
    } else {
        df = df.Define("event_uid", []() { return 0ULL; });
    }

    auto define_bool_alias = [](ROOT::RDF::RNode node, const std::string &target, const std::string &source) {
        return node.Define(target, [](const auto &value) { return static_cast<bool>(value); }, {source});
    };

    if (df.HasColumn("passes_preselection")) {
        df = define_bool_alias(df, "base_sel", "passes_preselection");
    } else if (df.HasColumn("pure_slice_signal")) {
        df = define_bool_alias(df, "base_sel", "pure_slice_signal");
    } else if (df.HasColumn("in_fiducial")) {
        df = define_bool_alias(df, "base_sel", "in_fiducial");
    } else {
        df = df.Define("base_sel", []() { return true; });
    }

    auto define_weight = [](ROOT::RDF::RNode node, const std::string &source) {
        return node.Define("w_nom", [](const auto &value) { return static_cast<double>(value); }, {source});
    };

    if (df.HasColumn("nominal_event_weight")) {
        df = define_weight(df, "nominal_event_weight");
    } else if (df.HasColumn("base_event_weight")) {
        df = define_weight(df, "base_event_weight");
    } else {
        df = df.Define("w_nom", []() { return 1.0; });
    }

    const ULong64_t friend_uid = static_cast<ULong64_t>(sampvar_uid);
    df = df.Define("is_mc", [is_mc]() { return is_mc; })
             .Define("sampvar_uid", [friend_uid]() { return friend_uid; });

    return df;
}

} // namespace

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
    if (!filter_expr.empty()) {
        log::info("SnapshotPipelineBuilder::snapshot", "[warning]",
                  "Selection filters are ignored when producing friend metadata:", filter_expr);
    }
    if (!columns.empty()) {
        log::info("SnapshotPipelineBuilder::snapshot", "[warning]",
                  "Requested payload columns are ignored; friend trees use a fixed schema.");
    }

    log::info("SnapshotPipelineBuilder::snapshot", "Preparing hub snapshot", output_file);
    log::info("SnapshotPipelineBuilder::snapshot", "Processing", frames_.size(), "samples");

    if (!frames_.empty()) {
        std::unordered_map<std::string, std::size_t> origin_counts;
        std::unordered_map<std::string, std::size_t> stage_counts;
        std::unordered_map<std::string, std::size_t> run_config_counts;
        origin_counts.reserve(frames_.size());
        stage_counts.reserve(frames_.size());
        run_config_counts.reserve(frames_.size());

        for (const auto &[sample_key, sample] : frames_) {
            ++origin_counts[proc::originToString(sample.sampleOrigin())];

            const std::string stage_label = sample.stageName().empty() ? std::string{"<none>"} : sample.stageName();
            ++stage_counts[stage_label];

            std::string run_label{"<unmapped>"};
            if (const auto *rc = this->getRunConfigForSample(sample_key)) {
                run_label = rc->label();
            }
            ++run_config_counts[run_label];
        }

        auto log_count_map = [](std::string_view heading,
                                 const std::unordered_map<std::string, std::size_t> &counts) {
            if (counts.empty()) {
                return;
            }
            std::vector<std::pair<std::string, std::size_t>> sorted_entries;
            sorted_entries.reserve(counts.size());
            for (const auto &entry : counts) {
                sorted_entries.emplace_back(entry.first, entry.second);
            }
            std::sort(sorted_entries.begin(), sorted_entries.end(),
                      [](const auto &lhs, const auto &rhs) { return lhs.first < rhs.first; });

            log::info("SnapshotPipelineBuilder::snapshot", "[debug]", heading);
            for (const auto &[label, count] : sorted_entries) {
                const char *sample_word = (count == 1) ? "sample" : "samples";
                std::ostringstream entry_message;
                entry_message << "  - " << label << " (" << count << ' ' << sample_word << ')';
                log::info("SnapshotPipelineBuilder::snapshot", "[debug]", entry_message.str());
            }
        };

        log_count_map("Sample distribution by origin:", origin_counts);
        log_count_map("Sample distribution by stage:", stage_counts);
        log_count_map("Sample distribution by run configuration:", run_config_counts);
    } else {
        log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "No samples have been queued for processing.");
    }

    // Build provenance dictionaries
    ProvenanceDicts dicts;
    dicts.origin2id[SampleOrigin::kData] =
        intern<decltype(dicts.origin2id), SampleOrigin, uint8_t>(dicts.origin2id, SampleOrigin::kData);
    dicts.origin2id[SampleOrigin::kMonteCarlo] =
        intern<decltype(dicts.origin2id), SampleOrigin, uint8_t>(dicts.origin2id, SampleOrigin::kMonteCarlo);
    dicts.origin2id[SampleOrigin::kDirt] =
        intern<decltype(dicts.origin2id), SampleOrigin, uint8_t>(dicts.origin2id, SampleOrigin::kDirt);
    dicts.origin2id[SampleOrigin::kExternal] =
        intern<decltype(dicts.origin2id), SampleOrigin, uint8_t>(dicts.origin2id, SampleOrigin::kExternal);

    // Build nodes
    std::vector<ROOT::RDF::RNode> nodes;
    nodes.reserve(frames_.size() * 2);
    std::vector<Combo> combos;
    combos.reserve(frames_.size() * 4);

    const std::vector<std::string> friend_columns = {"event_uid", "w_nom", "base_sel", "is_mc", "sampvar_uid"};

    for (const auto &[key, sample] : frames_) {
        const auto *rc = this->getRunConfigForSample(key);
        const std::string beam = rc ? rc->beamMode() : std::string{};
        const std::string period = rc ? rc->runPeriod() : std::string{};
        const std::string &stage = sample.stageName();
        const auto origin = sample.sampleOrigin();
        const std::string stage_label = stage.empty() ? std::string{"<none>"} : stage;
        const auto variation_nodes = sample.variationNodes().size();

        log::info("SnapshotPipelineBuilder::snapshot", "Configuring sample", key.str(), "origin",
                  originToString(origin), "stage", stage_label, "with", variation_nodes, "variation nodes");

        const uint32_t sid = intern<decltype(dicts.sample2id), std::string, uint32_t>(dicts.sample2id, key.str());
        const uint16_t bid = intern<decltype(dicts.beam2id), std::string, uint16_t>(dicts.beam2id, beam);
        const uint16_t pid = intern<decltype(dicts.period2id), std::string, uint16_t>(dicts.period2id, period);
        const uint16_t stg = intern<decltype(dicts.stage2id), std::string, uint16_t>(dicts.stage2id, stage);
        const uint16_t vnom = intern<decltype(dicts.var2id), std::string, uint16_t>(dicts.var2id, "nominal");
        const uint8_t oid = dicts.origin2id.at(origin);
        const bool is_mc = (origin == SampleOrigin::kMonteCarlo);

        // nominal
        {
            auto df = sample.nominalNode();
            df = configureFriendNode(df, is_mc, (static_cast<uint64_t>(sid) << 16) | vnom);
            nodes.emplace_back(df);

            combos.push_back(Combo{sid, vnom, bid, pid, stg, oid, origin, key.str(), "nominal", beam, period, stage,
                                   originToString(origin), sample.relativePath(), kInputTreeName, sample.pot(),
                                   sample.triggers(), true});
        }

        // variations
        for (const auto &vd : sample.variationDescriptors()) {
            auto it = sample.variationNodes().find(vd.variation);
            if (it == sample.variationNodes().end()) {
                continue;
            }

            const auto *vrc = this->getRunConfigForSample(vd.sample_key);
            const std::string vbeam = vrc ? vrc->beamMode() : beam;
            const std::string vperiod = vrc ? vrc->runPeriod() : period;
            const std::string &vstage = vd.stage_name.empty() ? stage : vd.stage_name;
            const std::string vstage_label = vstage.empty() ? std::string{"<none>"} : vstage;
            const std::string variation_label = variationLabelOrKey(vd);

            const uint16_t vbid = intern<decltype(dicts.beam2id), std::string, uint16_t>(dicts.beam2id, vbeam);
            const uint16_t vpid = intern<decltype(dicts.period2id), std::string, uint16_t>(dicts.period2id, vperiod);
            const uint16_t vstg = intern<decltype(dicts.stage2id), std::string, uint16_t>(dicts.stage2id, vstage);
            const uint16_t vvid = intern<decltype(dicts.var2id), std::string, uint16_t>(dicts.var2id, variation_label);

            log::info("SnapshotPipelineBuilder::snapshot", "Configuring variation", variation_label, "for sample",
                      key.str(), "stage", vstage_label);

            auto vdf = it->second;
            vdf = configureFriendNode(vdf, is_mc, (static_cast<uint64_t>(sid) << 16) | vvid);
            nodes.emplace_back(vdf);

            combos.push_back(Combo{sid, vvid, vbid, vpid, vstg, oid, origin, key.str(), variation_label, vbeam,
                                   vperiod, vstage, originToString(origin), vd.relative_path, kInputTreeName, vd.pot,
                                   vd.triggers, false});
        }
    }

    if (nodes.empty()) {
        log::info("SnapshotPipelineBuilder::snapshot", "[warning]", "No nodes to process.");
        return;
    }

    log::info("SnapshotPipelineBuilder::snapshot", "Prepared", combos.size(),
              "friend dataframe nodes for snapshot");

    snapshotToHub(output_file, friend_columns, nodes, combos, dicts);
}

void SnapshotPipelineBuilder::snapshot(const FilterExpression &query, const std::string &output_file,
                                       const std::vector<std::string> &columns) const {
    this->snapshot(query.str(), output_file, columns);
}

void SnapshotPipelineBuilder::snapshotToHub(const std::string &hub_path,
                                            const std::vector<std::string> &friend_columns,
                                            std::vector<ROOT::RDF::RNode> &nodes,
                                            const std::vector<Combo> &combos,
                                            const ProvenanceDicts &dicts) const {
    log::info("SnapshotPipelineBuilder", "Creating hub snapshot:", hub_path);

    HubCatalog hub(hub_path, true);
    hub.writeDictionaries(dicts);
    const std::string friend_tree_name = "meta";
    hub.writeSummary(total_pot_, total_triggers_, ntuple_base_directory_, friend_tree_name);

    FriendWriter::FriendConfig friend_config;
    const std::filesystem::path hub_dir = std::filesystem::absolute(std::filesystem::path(hub_path)).parent_path();
    friend_config.output_dir = hub_dir / "friends";

    FriendWriter writer(friend_config);

    std::vector<std::future<std::vector<HubEntry>>> futures;
    futures.reserve(nodes.size());

    for (std::size_t idx = 0; idx < nodes.size(); ++idx) {
        futures.push_back(std::async(std::launch::async, [&, idx]() {
            ROOT::EnableImplicitMT(1);
            auto node = nodes[idx];
            const auto &combo = combos[idx];

            log::info("SnapshotPipelineBuilder", "Materialising friend metadata for", combo.sk, combo.vlab);

            auto count = node.Count();
            auto min_uid = node.Min<ULong64_t>("event_uid");
            auto max_uid = node.Max<ULong64_t>("event_uid");
            auto sum_weights = node.Sum<double>("w_nom");

            auto path = writer.writeFriend(node, combo.sk, combo.vlab, friend_columns);

            const auto n_events = count.GetValue();
            if (n_events == 0ULL) {
                std::error_code remove_ec;
                std::filesystem::remove(path, remove_ec);
                if (remove_ec) {
                    log::info("SnapshotPipelineBuilder", "[warning]", "Failed to remove empty friend tree",
                              path.string(), remove_ec.message());
                }
                return std::vector<HubEntry>{};
            }

            HubEntry entry;
            entry.sample_id = combo.sid;
            entry.beam_id = combo.bid;
            entry.period_id = combo.pid;
            entry.variation_id = combo.vid;
            entry.origin_id = combo.oid;
            entry.dataset_path = combo.dataset_path;
            entry.dataset_tree = combo.dataset_tree;

            std::error_code rel_ec;
            const auto relative_friend = std::filesystem::relative(path, hub_dir, rel_ec);
            entry.friend_path = (rel_ec ? path : relative_friend).generic_string();
            entry.friend_tree = friend_tree_name;

            entry.n_events = n_events;
            entry.first_event_uid = min_uid->GetValue();
            entry.last_event_uid = max_uid->GetValue();
            entry.sum_weights = sum_weights->GetValue();
            entry.pot = combo.pot;
            entry.triggers = combo.triggers;
            entry.sample_key = combo.sk;
            entry.beam = combo.beam;
            entry.period = combo.period;
            entry.variation = combo.vlab;
            entry.origin = combo.origin_label;
            entry.stage = combo.stage;

            return std::vector<HubEntry>{std::move(entry)};
        }));
    }

    std::vector<HubEntry> all_entries;
    all_entries.reserve(nodes.size());
    for (auto &future : futures) {
        auto entries = future.get();
        all_entries.insert(all_entries.end(), std::make_move_iterator(entries.begin()),
                           std::make_move_iterator(entries.end()));
    }

    hub.addEntries(all_entries);
    hub.finalize();

    log::info("SnapshotPipelineBuilder", "Created", all_entries.size(),
              "hub entries with friend metadata:", hub_path);
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
    const auto sample_count = rc.sampleConfigs().size();
    log::info("SnapshotPipelineBuilder::processRunConfig", "Processing run configuration", rc.label(), "with",
              sample_count, "samples");
    processors_.reserve(processors_.size() + rc.sampleConfigs().size());
    for (auto &sample_json : rc.sampleConfigs()) {
        if (sample_json.contains("active") && !sample_json.at("active").get<bool>()) {
            log::info("SnapshotPipelineBuilder::processRunConfig", "Skipping inactive sample",
                      sample_json.at("sample_key").get<std::string>());
            continue;
        }

        auto pipeline = std::make_unique<ProcessorPipeline<WeightProcessor, TruthChannelProcessor, BlipProcessor,
                                                            MuonSelectionProcessor, ReconstructionProcessor,
                                                            PreselectionProcessor>>(
            std::make_unique<WeightProcessor>(sample_json, total_pot_, total_triggers_),
            std::make_unique<TruthChannelProcessor>(), std::make_unique<BlipProcessor>(),
            std::make_unique<MuonSelectionProcessor>(), std::make_unique<ReconstructionProcessor>(),
            std::make_unique<PreselectionProcessor>());
        processors_.push_back(std::move(pipeline));

        auto &processor = *processors_.back();

        SamplePipeline sample{sample_json, rc.sampleConfigs(), ntuple_base_directory_, var_registry_, processor};
        const auto sample_key = sample.sampleKey();

        log::info("SnapshotPipelineBuilder::processRunConfig", "Loaded sample", sample_key.str(), "for run config",
                  rc.label());

        run_config_cache_.emplace(sample_key, &rc);
        for (const auto &variation_def : sample.variationDescriptors()) {
            run_config_cache_.emplace(variation_def.sample_key, &rc);
        }
        frames_.emplace(sample_key, std::move(sample));
    }
}


} // namespace proc
