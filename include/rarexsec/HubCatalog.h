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

struct HubEntry {
    // Identity
    UInt_t entry_id = 0U;
    UInt_t sample_id = 0U;
    UShort_t beam_id = 0U;
    UShort_t period_id = 0U;
    UShort_t variation_id = 0U;
    UChar_t origin_id = 0U;

    // Location
    std::string dataset_path;
    std::string dataset_tree;
    std::string friend_path;
    std::string friend_tree;

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

struct HubFriend {
    UInt_t entry_id = 0U;
    std::string label;
    std::string tree;
    std::string path;
};

class HubCatalog {
  public:
    enum class OpenMode { Read, Update, Recreate };

    explicit HubCatalog(const std::string &hub_path, OpenMode mode = OpenMode::Read);
    ~HubCatalog();

    void addEntry(const HubEntry &entry);
    void addEntries(const std::vector<HubEntry> &entries);

    void addFriend(const HubFriend &friend_entry);
    void addFriends(const std::vector<HubFriend> &friend_entries);

    void writeDictionaries(const ProvenanceDicts &dicts);
    void writeSummary(double total_pot, long total_triggers, const std::string &base_directory,
                      const std::string &friend_tree_name);
    void finalize();

  private:
    std::unique_ptr<TFile> file_;
    TTree *catalog_tree_;
    TTree *meta_tree_;
    TTree *friend_tree_;
    mutable std::mutex mutex_;

    HubEntry current_entry_;
    HubFriend current_friend_;
    UInt_t next_entry_id_;
    std::string meta_key_;
    std::string meta_value_;
    bool finalized_;
};

} // namespace proc

#endif
