#include <rarexsec/SnapshotPipelineBuilder.h>

#include "TDirectory.h"
#include "TFile.h"
#include "TROOT.h"
#include "TTree.h"
#include "TObject.h"
#include "TSystem.h"
#include "ROOT/RDataFrame.hxx"
#include "ROOT/RDFHelpers.hxx"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <rarexsec/BlipProcessor.h>
#include <rarexsec/LoggerUtils.h>
#include <rarexsec/MuonSelectionProcessor.h>
#include <rarexsec/PreselectionProcessor.h>
#include <rarexsec/ReconstructionProcessor.h>
#include <rarexsec/SampleTypes.h>
#include <rarexsec/TruthChannelProcessor.h>
#include <rarexsec/WeightProcessor.h>

namespace {

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

namespace snapshot_detail {
template <typename T, typename = void>
struct has_autoflush : std::false_type {};
template <typename T>
struct has_autoflush<T, std::void_t<decltype(std::declval<T &>().fAutoFlush)>> : std::true_type {};
template <typename T>
constexpr bool has_autoflush_v = has_autoflush<T>::value;

template <typename T, typename = void>
struct has_overwrite : std::false_type {};
template <typename T>
struct has_overwrite<T, std::void_t<decltype(std::declval<T &>().fOverwriteIfExists)>> : std::true_type {};
template <typename T>
constexpr bool has_overwrite_v = has_overwrite<T>::value;
} // namespace snapshot_detail

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

static void appendPathSegment(std::string &path, std::string_view segment) {
    if (segment.empty()) {
        return;
    }
    if (!path.empty() && path.back() != '/') {
        path.push_back('/');
    }
    path.append(segment.data(), segment.size());
}

static std::string nominalTreePath(const proc::SampleKey &key, const std::string &beam, const std::string &run_period,
                                   proc::SampleOrigin origin, const std::string &stage_name) {
    std::string path = "samples";
    appendPathSegment(path, beam);
    appendPathSegment(path, run_period);
    const std::string origin_label = proc::originToString(origin);
    appendPathSegment(path, origin_label);
    appendPathSegment(path, stage_name);
    appendPathSegment(path, key.str());
    appendPathSegment(path, "nominal");
    appendPathSegment(path, "events");
    return path;
}

static std::string variationTreePath(const proc::SampleKey &key, const proc::VariationDescriptor &vd,
                                     const std::string &beam, const std::string &run_period, proc::SampleOrigin origin,
                                     const std::string &stage_name) {
    std::string path = "samples";
    appendPathSegment(path, beam);
    appendPathSegment(path, run_period);
    const std::string origin_label = proc::originToString(origin);
    appendPathSegment(path, origin_label);
    appendPathSegment(path, stage_name);
    appendPathSegment(path, key.str());
    const std::string variation_label = variationLabelOrKey(vd);
    appendPathSegment(path, variation_label);
    appendPathSegment(path, "events");
    return path;
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
    struct Combo {
        uint32_t sid;
        uint16_t vid, bid, pid, stg;
        uint8_t oid;
        std::string sk, vlab, beam, period, stage, origin;
    };
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

            combos.push_back(Combo{sid, vnom, bid, pid, stg, oid, key.str(), "nominal", beam, period, stage,
                                   originToString(origin)});
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

            combos.push_back(Combo{sid, vvid, vbid, vpid, vstg, oid, key.str(), variation_label, vbeam, vperiod,
                                   vstage, originToString(origin)});
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

    ROOT::RDF::RSnapshotOptions base_opt;
    base_opt.fMode = "RECREATE";
    base_opt.fLazy = false;
    base_opt.fCompressionAlgorithm = ROOT::kLZ4;
    base_opt.fCompressionLevel = 1;
    if constexpr (snapshot_detail::has_autoflush_v<ROOT::RDF::RSnapshotOptions>) {
        base_opt.fAutoFlush = 100000;
    }
    if constexpr (snapshot_detail::has_overwrite_v<ROOT::RDF::RSnapshotOptions>) {
        base_opt.fOverwriteIfExists = true;
    }

    // Per-(sample, variation) results, one dataframe per node
    using SnapshotResultHandle = decltype(std::declval<ROOT::RDF::RNode>().Snapshot(
        std::declval<std::string>(), std::declval<std::string>(),
        std::declval<std::vector<std::string>>(), std::declval<ROOT::RDF::RSnapshotOptions>()));

    struct CountHandles {
        CutflowRow row;
        SnapshotResultHandle snapshot;
        ROOT::RDF::RResultPtr<ULong64_t> n_total;
        ROOT::RDF::RResultPtr<ULong64_t> n_base;
    };
    std::vector<CountHandles> counts;
    counts.reserve(combos.size());
    for (std::size_t idx = 0; idx < combos.size(); ++idx) {
        const auto &c = combos[idx];

        CountHandles ch;
        ch.row.sample_id = c.sid;
        ch.row.variation_id = c.vid;
        ch.row.beam_id = c.bid;
        ch.row.period_id = c.pid;
        ch.row.stage_id = c.stg;
        ch.row.origin_id = c.oid;
        ch.row.sample_key = c.sk;
        ch.row.variation = c.vlab;
        ch.row.beam = c.beam;
        ch.row.period = c.period;
        ch.row.stage = c.stage;
        ch.row.origin = c.origin;

        ROOT::RDF::RSnapshotOptions opt = base_opt;
        if (idx > 0) {
            opt.fMode = "UPDATE";
        }

        auto &node = nodes[idx];
        ch.snapshot = node.Snapshot("events", output_file, final_cols, opt);
        ch.n_total = node.Count();
        ch.n_base = node.Filter("base_sel").Count();
        counts.emplace_back(std::move(ch));
    }

    constexpr std::size_t BATCH_SIZE = 5;
    const std::size_t total_datasets = counts.size();
    const std::size_t total_batches = total_datasets == 0 ? 0 : (total_datasets + BATCH_SIZE - 1) / BATCH_SIZE;
    log::info("SnapshotPipelineBuilder::snapshot", "Executing snapshot for", total_datasets,
              "datasets in batches of", BATCH_SIZE);

    for (std::size_t batch_start = 0; batch_start < total_datasets; batch_start += BATCH_SIZE) {
        const std::size_t batch_end = std::min(batch_start + BATCH_SIZE, total_datasets);
        std::vector<ROOT::RDF::RResultHandle> batch_handles;
        batch_handles.reserve((batch_end - batch_start) * 3);

        for (std::size_t idx = batch_start; idx < batch_end; ++idx) {
            auto &ch = counts[idx];
            const std::size_t step = idx + 1;
            log::info("SnapshotPipelineBuilder::snapshot", "Queuing dataset", step, "/", total_datasets,
                      "- sample", ch.row.sample_key, "variation", ch.row.variation);
            batch_handles.emplace_back(ch.snapshot);
            batch_handles.emplace_back(ch.n_total);
            batch_handles.emplace_back(ch.n_base);
        }

        if (!batch_handles.empty()) {
            const std::size_t batch_number = (batch_start / BATCH_SIZE) + 1;
            log::info("SnapshotPipelineBuilder::snapshot", "Executing batch", batch_number, "/", total_batches);
            ROOT::RDF::RunGraphs(batch_handles);
        }

        for (std::size_t idx = batch_start; idx < batch_end; ++idx) {
            auto &ch = counts[idx];
            const std::size_t step = idx + 1;
            const auto total_events = static_cast<unsigned long long>(*ch.n_total);
            const auto base_events = static_cast<unsigned long long>(*ch.n_base);
            log::info("SnapshotPipelineBuilder::snapshot", "Completed dataset", step, "/", total_datasets,
                      "- sample", ch.row.sample_key, "variation", ch.row.variation, "events processed:",
                      total_events, "base selection:", base_events);
        }

        if (batch_end < total_datasets) {
            gSystem->ProcessEvents();
            if (auto canvases = gROOT->GetListOfCanvases()) {
                canvases->Delete();
            }
            if (TDirectory *dir = gDirectory) {
                if (auto dir_list = dir->GetList()) {
                    dir_list->Delete();
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    std::vector<CutflowRow> cutflow;
    cutflow.reserve(counts.size());
    for (auto &ch : counts) {
        ch.row.n_total = static_cast<unsigned long long>(*ch.n_total);
        ch.row.n_base = static_cast<unsigned long long>(*ch.n_base);
        cutflow.emplace_back(std::move(ch.row));
    }

    // Write metadata + index
    {
        log::info("SnapshotPipelineBuilder::snapshot", "Writing metadata tables to", output_file);
        ImplicitMTGuard imt_guard;
        std::unique_ptr<TFile> f(TFile::Open(output_file.c_str(), "UPDATE"));
        if (!f || f->IsZombie()) {
            log::fatal("SnapshotPipelineBuilder::snapshot", "Failed to reopen output for metadata:", output_file);
        }
        if (auto t = dynamic_cast<TTree *>(f->Get("events"))) {
            if (t->GetBranch("rsub_key") && t->GetBranch("evt")) {
                log::info("SnapshotPipelineBuilder::snapshot", "[debug]", "Building TTree index (rsub_key, evt)");
                t->BuildIndex("rsub_key", "evt");
                t->Write("", TObject::kOverwrite);
            }
        }
        this->writeSnapshotMetadata(*f, dicts, cutflow);
    }

    log::info("SnapshotPipelineBuilder::snapshot", "Snapshot generation completed:", output_file);
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

        auto pipeline = this->chainProcessorStages(
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

void SnapshotPipelineBuilder::writeSnapshotMetadata(TFile &output_file,
                                                    const ProvenanceDicts &dicts,
                                                    const std::vector<CutflowRow> &cutflow) const {
    ImplicitMTGuard imt_guard;

    TDirectory *meta_dir = output_file.GetDirectory("meta");
    if (!meta_dir) meta_dir = output_file.mkdir("meta");
    if (!meta_dir) {
        log::fatal("SnapshotPipelineBuilder::writeSnapshotMetadata", "Could not create meta directory in",
                   output_file.GetName());
    }
    meta_dir->cd();

    // Schema tag
    {
        TTree v("schema", "schema");
        int version = 1;
        v.Branch("version", &version);
        v.Fill();
        v.Write("", TObject::kOverwrite);
    }

    // Totals
    {
        TTree totals_tree("totals", "Exposure totals");
        double total_pot = total_pot_;
        long total_triggers = total_triggers_;
        totals_tree.Branch("total_pot", &total_pot);
        totals_tree.Branch("total_triggers", &total_triggers);
        totals_tree.Fill();
        totals_tree.Write("", TObject::kOverwrite);
    }

    // Dictionaries (beams/periods/stages/variations)
    auto write_string_dict = [&](const char *name, const auto &map_in) {
        std::vector<std::pair<uint32_t,std::string>> rows;
        rows.reserve(map_in.size());
        for (const auto &kv : map_in) rows.emplace_back(kv.second, kv.first);
        std::sort(rows.begin(), rows.end(), [](auto &a, auto &b){ return a.first < b.first; });

        TTree t(name, name);
        uint32_t id; std::string name_s;
        t.Branch("id",   &id);
        t.Branch("name", &name_s);
        for (auto &r : rows) { id = r.first; name_s = r.second; t.Fill(); }
        t.Write("", TObject::kOverwrite);
    };
    write_string_dict("beams",      dicts.beam2id);
    write_string_dict("periods",    dicts.period2id);
    write_string_dict("stages",     dicts.stage2id);
    write_string_dict("variations", dicts.var2id);

    // Samples dict (id <-> key)
    {
        std::vector<std::pair<uint32_t,std::string>> rows;
        rows.reserve(dicts.sample2id.size());
        for (const auto &kv : dicts.sample2id) rows.emplace_back(kv.second, kv.first);
        std::sort(rows.begin(), rows.end(), [](auto &a, auto &b){ return a.first < b.first; });

        TTree t("samples_dict", "samples_dict");
        uint32_t id; std::string key;
        t.Branch("id",  &id);
        t.Branch("key", &key);
        for (auto &r : rows) { id = r.first; key = r.second; t.Fill(); }
        t.Write("", TObject::kOverwrite);
    }

    // Origins dict
    {
        TTree t("origins", "origins");
        uint8_t id; std::string name_s;
        t.Branch("id",   &id);
        t.Branch("name", &name_s);
        auto write_one = [&](SampleOrigin o, const char *nm) {
            auto it = dicts.origin2id.find(o);
            if (it == dicts.origin2id.end()) return;
            id = it->second; name_s = nm; t.Fill();
        };
        write_one(SampleOrigin::kData,      "data");
        write_one(SampleOrigin::kMonteCarlo,"mc");
        write_one(SampleOrigin::kDirt,      "dirt");
        write_one(SampleOrigin::kExternal,  "external");
        t.Write("", TObject::kOverwrite);
    }

    // Per-sample exposure summary (augmented with IDs)
    {
        TTree samples_tree("samples", "Per-sample exposure summary");
        std::string tree_path, sample_key_value, beam, run_period, relative_path, variation_label, stage_name, origin_label;
        double sample_pot; long sample_triggers;
        uint32_t sample_id; uint16_t beam_id, period_id, stage_id, variation_id; uint8_t origin_id;

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
        samples_tree.Branch("sample_id", &sample_id);
        samples_tree.Branch("beam_id", &beam_id);
        samples_tree.Branch("period_id", &period_id);
        samples_tree.Branch("stage_id", &stage_id);
        samples_tree.Branch("variation_id", &variation_id);
        samples_tree.Branch("origin_id", &origin_id);

        for (auto const &[key, sample] : frames_) {
            const auto *rc = this->getRunConfigForSample(key);
            const std::string sample_beam   = rc ? rc->beamMode()  : std::string{};
            const std::string sample_period = rc ? rc->runPeriod() : std::string{};
            const std::string &sample_stage = sample.stageName();

            beam = sample_beam; run_period = sample_period; stage_name = sample_stage;
            tree_path = nominalTreePath(key, sample_beam, sample_period, sample.sampleOrigin(), sample_stage);
            sample_key_value = key.str();
            relative_path = sample.relativePath();
            variation_label = "nominal";
            origin_label = originToString(sample.sampleOrigin());
            sample_pot = sample.pot(); sample_triggers = sample.triggers();

            sample_id  = dicts.sample2id.at(sample_key_value);
            beam_id    = dicts.beam2id.at(beam);
            period_id  = dicts.period2id.at(run_period);
            stage_id   = dicts.stage2id.at(stage_name);
            variation_id = dicts.var2id.at(variation_label);
            origin_id  = dicts.origin2id.at(sample.sampleOrigin());
            samples_tree.Fill();

            for (const auto &vd : sample.variationDescriptors()) {
                const auto *vrc = this->getRunConfigForSample(vd.sample_key);
                const std::string vbeam   = vrc ? vrc->beamMode()  : sample_beam;
                const std::string vperiod = vrc ? vrc->runPeriod() : sample_period;
                const std::string &vstage = vd.stage_name.empty() ? sample_stage : vd.stage_name;

                beam = vbeam; run_period = vperiod; stage_name = vstage;
                variation_label = variationLabelOrKey(vd);
                tree_path = variationTreePath(key, vd, vbeam, vperiod, sample.sampleOrigin(), vstage);
                sample_key_value = vd.sample_key.str();
                relative_path = vd.relative_path;
                origin_label = originToString(sample.sampleOrigin());
                sample_pot = vd.pot; sample_triggers = vd.triggers;

                sample_id  = dicts.sample2id.at(key.str()); // same base sample id
                beam_id    = dicts.beam2id.at(beam);
                period_id  = dicts.period2id.at(run_period);
                stage_id   = dicts.stage2id.at(stage_name);
                variation_id = dicts.var2id.at(variation_label);
                origin_id  = dicts.origin2id.at(sample.sampleOrigin());
                samples_tree.Fill();
            }
        }
        samples_tree.Write("", TObject::kOverwrite);
    }

    // Cutflow
    {
        TTree t("cutflow", "Cutflow per sample/variation");
        uint32_t sample_id;  uint16_t variation_id, beam_id, period_id, stage_id;  uint8_t origin_id;
        unsigned long long n_total, n_base;
        std::string sample_key, variation, beam, period, stage, origin;

        t.Branch("sample_id",   &sample_id);
        t.Branch("variation_id",&variation_id);
        t.Branch("beam_id",     &beam_id);
        t.Branch("period_id",   &period_id);
        t.Branch("stage_id",    &stage_id);
        t.Branch("origin_id",   &origin_id);
        t.Branch("n_total",     &n_total);
        t.Branch("n_base",      &n_base);
        t.Branch("sample_key",  &sample_key);
        t.Branch("variation",   &variation);
        t.Branch("beam",        &beam);
        t.Branch("period",      &period);
        t.Branch("stage",       &stage);
        t.Branch("origin",      &origin);

        for (const auto &r : cutflow) {
            sample_id   = r.sample_id;
            variation_id= r.variation_id;
            beam_id     = r.beam_id;
            period_id   = r.period_id;
            stage_id    = r.stage_id;
            origin_id   = r.origin_id;
            n_total     = r.n_total;
            n_base      = r.n_base;
            sample_key  = r.sample_key;
            variation   = r.variation;
            beam        = r.beam;
            period      = r.period;
            stage       = r.stage;
            origin      = r.origin;
            t.Fill();
        }
        t.Write("", TObject::kOverwrite);
    }

    output_file.cd();
}

} // namespace proc
