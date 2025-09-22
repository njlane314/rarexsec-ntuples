#ifndef HUB_CATALOG_H
#define HUB_CATALOG_H

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "TFile.h"
#include "TTree.h"

namespace proc {

struct ProvenanceDicts;

struct ShardEntry {
    // Identity
    UInt_t shard_id = 0U;
    UInt_t sample_id = 0U;
    UShort_t beam_id = 0U;
    UShort_t period_id = 0U;
    UShort_t variation_id = 0U;
    UChar_t origin_id = 0U;

    // Location
    std::string shard_path;
    std::string tree_name;

    // Summary
    ULong64_t n_events = 0ULL;
    ULong64_t first_event_uid = 0ULL;
    ULong64_t last_event_uid = 0ULL;
    Double_t sum_weights = 0.0;
    Double_t pot = 0.0;
    Long64_t triggers = 0;

    // Query keys
    std::string sample_key;
    std::string beam;
    std::string period;
    std::string variation;
    std::string origin;
    std::string stage;
};

class HubCatalog {
  public:
    explicit HubCatalog(const std::string &hub_path, bool create = false);
    ~HubCatalog();

    void addShardEntry(const ShardEntry &entry);
    void addShardEntries(const std::vector<ShardEntry> &entries);
    std::vector<std::string> queryShardPaths(const std::string &selection) const;

    void writeDictionaries(const ProvenanceDicts &dicts);
    void writeSummary(double total_pot, long total_triggers);
    void finalize();

  private:
    std::unique_ptr<TFile> file_;
    TTree *catalog_tree_;
    TTree *meta_tree_;
    mutable std::mutex mutex_;

    ShardEntry current_entry_;
    UInt_t next_shard_id_;
    std::string meta_key_;
    std::string meta_value_;
    bool finalized_;
};

} // namespace proc

#endif
