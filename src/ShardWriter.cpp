#include <rarexsec/ShardWriter.h>

#include <rarexsec/LoggerUtils.h>

#include <filesystem>
#include <iomanip>
#include <memory>
#include <sstream>
#include <system_error>
#include <vector>

namespace proc {

ShardWriter::ShardWriter(const ShardConfig &config) : config_(config) {
    std::error_code ec;
    std::filesystem::create_directories(config_.output_dir, ec);
    if (ec) {
        log::info("ShardWriter", "[warning]", "Failed to ensure shard output directory", config_.output_dir.string(),
                  ":", ec.message());
    }
}

std::vector<ShardEntry> ShardWriter::writeShards(
    ROOT::RDF::RNode &df,
    const std::string &sample_key,
    const std::string &beam,
    const std::string &period,
    const std::string &variation,
    const std::string &stage,
    SampleOrigin origin,
    double dataset_pot,
    long dataset_triggers,
    const std::vector<std::string> &columns) {

    std::vector<ShardEntry> entries;

    const ULong64_t n_total = df.Count().GetValue();
    if (n_total == 0ULL) {
        log::info("ShardWriter", "No events for", sample_key, variation);
        return entries;
    }

    ROOT::RDF::RSnapshotOptions opt;
    opt.fCompressionAlgorithm = config_.compression_algo;
    opt.fCompressionLevel = config_.compression_level;
    opt.fAutoFlush = -30000000;

    const bool has_uid = df.HasColumn("event_uid");
    const bool has_weight = df.HasColumn("w_nom");

    auto shard_path = generateShardPath(sample_key, variation, 0);

    std::unique_ptr<ROOT::RDF::RResultPtr<ULong64_t>> min_uid;
    std::unique_ptr<ROOT::RDF::RResultPtr<ULong64_t>> max_uid;
    std::unique_ptr<ROOT::RDF::RResultPtr<double>> sum_weights;

    if (has_uid) {
        min_uid = std::make_unique<ROOT::RDF::RResultPtr<ULong64_t>>(df.Min<ULong64_t>("event_uid"));
        max_uid = std::make_unique<ROOT::RDF::RResultPtr<ULong64_t>>(df.Max<ULong64_t>("event_uid"));
    }
    if (has_weight) {
        sum_weights = std::make_unique<ROOT::RDF::RResultPtr<double>>(df.Sum<double>("w_nom"));
    }

    auto snap = df.Snapshot("events", shard_path.string(), columns, opt);
    snap.GetValue();

    ShardEntry entry;
    entry.shard_path = shard_path.string();
    entry.tree_name = "events";
    entry.n_events = n_total;
    entry.sample_key = sample_key;
    entry.beam = beam;
    entry.period = period;
    entry.variation = variation;
    entry.origin = originToString(origin);
    entry.stage = stage;
    entry.pot = dataset_pot;
    entry.triggers = dataset_triggers;

    if (min_uid) {
        entry.first_event_uid = min_uid->GetValue();
        entry.last_event_uid = max_uid->GetValue();
    }
    if (sum_weights) {
        entry.sum_weights = sum_weights->GetValue();
    }

    entries.emplace_back(std::move(entry));
    log::info("ShardWriter", "Wrote shard for", sample_key, variation, "with", n_total, "events");

    return entries;
}

std::filesystem::path ShardWriter::generateShardPath(
    const std::string &sample_key,
    const std::string &variation,
    uint32_t shard_index) const {
    std::ostringstream filename;
    filename << sample_key << "_" << variation << "_" << std::setfill('0') << std::setw(4) << shard_index << ".root";
    return config_.output_dir / filename.str();
}

} // namespace proc
