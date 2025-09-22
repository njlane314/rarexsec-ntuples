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
#include <rarexsec/ShardWriter.h>
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

static ROOT::RDF::RNode defineProvenanceIDs(ROOT::RDF::RNode df, uint32_t sample_id, uint16_t beam_id,
                                            uint16_t period_id, uint16_t stage_id, uint16_t variation_id,
                                            uint8_t origin_id) {
    return df.Define("sample_id", [sample_id]() { return sample_id; })
        .Define("beam_id", [beam_id]() { return beam_id; })
        .Define("period_id", [period_id]() { return period_id; })
        .Define("stage_id", [stage_id]() { return stage_id; })
        .Define("variation_id", [variation_id]() { return variation_id; })
        .Define("origin_id", [origin_id]() { return origin_id; });
}

static ROOT::RDF::RNode defineAnalysisAliases(ROOT::RDF::RNode df) {
    const auto column_names = df.GetColumnNames();
    const std::unordered_set<std::string> column_set(column_names.begin(), column_names.end());

    const bool has_run = column_set.count("run") > 0;
    const bool has_sub = column_set.count("sub") > 0;
    const bool has_evt = column_set.count("evt") > 0;
    if (has_run && has_sub && has_evt) {
        df = df.Define("event_uid",
                       "((ULong64_t)run << 42) | ((ULong64_t)sub << 21) | (ULong64_t)evt")
                 .Define("rsub_key", "((ULong64_t)run << 20) | (ULong64_t)sub");
    } else {
        df = df.Define("event_uid", "0ULL").Define("rsub_key", "0ULL");
    }

    if (column_set.count("passes_preselection") > 0) {
        df = df.Alias("base_sel", "passes_preselection");
    } else if (column_set.count("pure_slice_signal") > 0) {
        df = df.Alias("base_sel", "pure_slice_signal");
    } else if (column_set.count("in_fiducial") > 0) {
        df = df.Alias("base_sel", "in_fiducial");
    } else {
        df = df.Define("base_sel", "true");
    }

    if (column_set.count("nominal_event_weight") > 0) {
        df = df.Define("w_nom", "(double)nominal_event_weight");
    } else if (column_set.count("base_event_weight") > 0) {
        df = df.Define("w_nom", "(double)base_event_weight");
    } else {
        df = df.Define("w_nom", "1.0");
    }

    return df;
}

static ROOT::RDF::RNode defineDownstreamConvenience(ROOT::RDF::RNode df, uint32_t sample_id, uint16_t variation_id,
                                                    bool is_mc, bool is_nominal, double sample_pot,
                                                    long sample_triggers) {
    const uint64_t svuid = (static_cast<uint64_t>(sample_id) << 16) | variation_id;
    return df.Define("is_mc", [is_mc]() { return is_mc; })
        .Define("is_nominal", [is_nominal]() { return is_nominal; })
        .Define("sampvar_uid", [svuid]() { return svuid; })
        .Define("sample_pot", [sample_pot]() { return sample_pot; })
        .Define("sample_triggers",
                [sample_triggers]() { return static_cast<ULong64_t>(sample_triggers); });
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
    // Deduplicate requested payload columns
    std::vector<std::string> requested = columns;
    {
        std::unordered_set<std::string> seen;
        std::vector<std::string> tmp;
        tmp.reserve(requested.size());
        for (auto &c : requested) {
            if (seen.insert(c).second) {
                tmp.emplace_back(std::move(c));
            }
        }
        requested.swap(tmp);
    }

    const std::string filter_description = filter_expr.empty() ? std::string{"<none>"} : filter_expr;
    log::info("SnapshotPipelineBuilder::snapshot", "Preparing snapshot", output_file, "with", requested.size(),
              "payload columns and filter", filter_description);
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
            if (!filter_expr.empty()) {
                df = df.Filter(filter_expr);
            }
            df = defineProvenanceIDs(df, sid, bid, pid, stg, vnom, oid);
            df = defineAnalysisAliases(df);
            df = defineDownstreamConvenience(df, sid, vnom, is_mc, /*is_nominal*/ true, sample.pot(),
                                             sample.triggers());
            nodes.emplace_back(df);

            combos.push_back(Combo{sid, vnom, bid, pid, stg, oid, origin, key.str(), "nominal", beam, period,
                                   stage, originToString(origin), sample.pot(), sample.triggers()});
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
            if (!filter_expr.empty()) {
                vdf = vdf.Filter(filter_expr);
            }
            vdf = defineProvenanceIDs(vdf, sid, vbid, vpid, vstg, vvid, oid);
            vdf = defineAnalysisAliases(vdf);
            vdf = defineDownstreamConvenience(vdf, sid, vvid, is_mc, /*is_nominal*/ false, vd.pot, vd.triggers);
            nodes.emplace_back(vdf);

            combos.push_back(Combo{sid, vvid, vbid, vpid, vstg, oid, origin, key.str(), variation_label, vbeam,
                                   vperiod, vstage, originToString(origin), vd.pot, vd.triggers});
        }
    }

    if (nodes.empty()) {
        log::info("SnapshotPipelineBuilder::snapshot", "[warning]", "No nodes to process.");
        return;
    }

    log::info("SnapshotPipelineBuilder::snapshot", "Prepared", combos.size(), "dataframe nodes for snapshot");

    // Final columns (payload + provenance + helpers)
    std::vector<std::string> final_cols = requested;
    final_cols.insert(final_cols.end(),
                      {"sample_id",       "beam_id",      "period_id",  "stage_id",   "variation_id",
                       "origin_id",      "event_uid",    "rsub_key",   "base_sel",   "w_nom",
                       "is_mc",          "is_nominal",   "sampvar_uid", "sample_pot", "sample_triggers"});

    snapshotToHub(output_file, final_cols, nodes, combos, dicts);
}

void SnapshotPipelineBuilder::snapshot(const FilterExpression &query, const std::string &output_file,
                                       const std::vector<std::string> &columns) const {
    this->snapshot(query.str(), output_file, columns);
}

void SnapshotPipelineBuilder::snapshotToHub(const std::string &hub_path,
                                            const std::vector<std::string> &final_columns,
                                            std::vector<ROOT::RDF::RNode> &nodes,
                                            const std::vector<Combo> &combos,
                                            const ProvenanceDicts &dicts) const {
    log::info("SnapshotPipelineBuilder", "Creating hub snapshot:", hub_path);

    HubCatalog hub(hub_path, true);
    hub.writeDictionaries(dicts);
    hub.writeSummary(total_pot_, total_triggers_);

    ShardWriter::ShardConfig shard_config;
    const std::filesystem::path hub_dir = std::filesystem::absolute(std::filesystem::path(hub_path)).parent_path();
    shard_config.output_dir = hub_dir / "shards";

    ShardWriter writer(shard_config);

    std::vector<std::future<std::vector<ShardEntry>>> futures;
    futures.reserve(nodes.size());

    for (std::size_t idx = 0; idx < nodes.size(); ++idx) {
        futures.push_back(std::async(std::launch::async, [&, idx]() {
            ROOT::EnableImplicitMT(1);
            auto node = nodes[idx];
            const auto &combo = combos[idx];

            log::info("SnapshotPipelineBuilder", "Materialising shard for", combo.sk, combo.vlab);

            auto entries = writer.writeShards(node, combo.sk, combo.beam, combo.period, combo.vlab, combo.stage,
                                              combo.origin_enum, combo.pot, combo.triggers, final_columns);

            for (auto &entry : entries) {
                entry.sample_id = combo.sid;
                entry.beam_id = combo.bid;
                entry.period_id = combo.pid;
                entry.variation_id = combo.vid;
                entry.origin_id = combo.oid;
                entry.pot = combo.pot;
                entry.triggers = combo.triggers;
            }

            return entries;
        }));
    }

    std::vector<ShardEntry> all_entries;
    for (auto &future : futures) {
        auto entries = future.get();
        all_entries.insert(all_entries.end(), std::make_move_iterator(entries.begin()),
                           std::make_move_iterator(entries.end()));
    }

    hub.addShardEntries(all_entries);
    hub.finalize();

    log::info("SnapshotPipelineBuilder", "Created", all_entries.size(), "shards referenced in hub:", hub_path);
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
