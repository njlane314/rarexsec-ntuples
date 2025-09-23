#include <rarexsec/HubCatalog.h>

#include <rarexsec/SampleTypes.h>
#include <rarexsec/SnapshotPipelineBuilder.h>

#include "TObject.h"
#include "TBranch.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace proc {
namespace {
constexpr const char *kCatalogTreeName = "entries";
constexpr const char *kMetaTreeName = "hub_meta";
constexpr const char *kFriendTreeName = "entry_friends";
constexpr const char *kCatalogTreeTitle = "Hub entries";
constexpr const char *kMetaTreeTitle = "Hub metadata";
constexpr const char *kFriendTreeTitle = "Entry friend metadata";

template <typename T>
void ensureBranch(TTree *tree, const char *name, T *address) {
    if (!tree) {
        return;
    }
    if (auto *branch = tree->GetBranch(name)) {
        branch->SetAddress(address);
    } else {
        tree->Branch(name, address);
    }
}
} // namespace

HubCatalog::HubCatalog(const std::string &hub_path, OpenMode mode)
    : catalog_tree_(nullptr),
      meta_tree_(nullptr),
      friend_tree_(nullptr),
      next_entry_id_(0U),
      finalized_(false) {
    const char *open_mode = nullptr;
    switch (mode) {
    case OpenMode::Recreate:
        open_mode = "RECREATE";
        break;
    case OpenMode::Update:
        open_mode = "UPDATE";
        break;
    case OpenMode::Read:
    default:
        open_mode = "READ";
        break;
    }

    file_.reset(TFile::Open(hub_path.c_str(), open_mode));
    if (!file_ || file_->IsZombie()) {
        throw std::runtime_error("Failed to open hub catalog file: " + hub_path);
    }

    if (mode == OpenMode::Recreate) {
        file_->cd();
        catalog_tree_ = new TTree(kCatalogTreeName, kCatalogTreeTitle);
        catalog_tree_->SetDirectory(file_.get());
        ensureBranch(catalog_tree_, "entry_id", &current_entry_.entry_id);
        ensureBranch(catalog_tree_, "sample_id", &current_entry_.sample_id);
        ensureBranch(catalog_tree_, "beam_id", &current_entry_.beam_id);
        ensureBranch(catalog_tree_, "period_id", &current_entry_.period_id);
        ensureBranch(catalog_tree_, "variation_id", &current_entry_.variation_id);
        ensureBranch(catalog_tree_, "origin_id", &current_entry_.origin_id);
        ensureBranch(catalog_tree_, "dataset_path", &current_entry_.dataset_path);
        ensureBranch(catalog_tree_, "dataset_tree", &current_entry_.dataset_tree);
        ensureBranch(catalog_tree_, "friend_path", &current_entry_.friend_path);
        ensureBranch(catalog_tree_, "friend_tree", &current_entry_.friend_tree);
        ensureBranch(catalog_tree_, "n_events", &current_entry_.n_events);
        ensureBranch(catalog_tree_, "first_event_uid", &current_entry_.first_event_uid);
        ensureBranch(catalog_tree_, "last_event_uid", &current_entry_.last_event_uid);
        ensureBranch(catalog_tree_, "sum_weights", &current_entry_.sum_weights);
        ensureBranch(catalog_tree_, "pot", &current_entry_.pot);
        ensureBranch(catalog_tree_, "triggers", &current_entry_.triggers);
        ensureBranch(catalog_tree_, "sample_key", &current_entry_.sample_key);
        ensureBranch(catalog_tree_, "beam", &current_entry_.beam);
        ensureBranch(catalog_tree_, "period", &current_entry_.period);
        ensureBranch(catalog_tree_, "variation", &current_entry_.variation);
        ensureBranch(catalog_tree_, "origin", &current_entry_.origin);
        ensureBranch(catalog_tree_, "stage", &current_entry_.stage);

        meta_tree_ = new TTree(kMetaTreeName, kMetaTreeTitle);
        meta_tree_->SetDirectory(file_.get());
        ensureBranch(meta_tree_, "key", &meta_key_);
        ensureBranch(meta_tree_, "value", &meta_value_);

        friend_tree_ = new TTree(kFriendTreeName, kFriendTreeTitle);
        friend_tree_->SetDirectory(file_.get());
        ensureBranch(friend_tree_, "entry_id", &current_friend_.entry_id);
        ensureBranch(friend_tree_, "label", &current_friend_.label);
        ensureBranch(friend_tree_, "tree", &current_friend_.tree);
        ensureBranch(friend_tree_, "path", &current_friend_.path);

        meta_key_ = "hub_version";
        meta_value_ = "1";
        meta_tree_->Fill();
    } else {
        catalog_tree_ = dynamic_cast<TTree *>(file_->Get(kCatalogTreeName));
        meta_tree_ = dynamic_cast<TTree *>(file_->Get(kMetaTreeName));
        friend_tree_ = dynamic_cast<TTree *>(file_->Get(kFriendTreeName));
        if (!catalog_tree_) {
            throw std::runtime_error("Hub catalog is missing the entries tree");
        }
        if (mode == OpenMode::Update) {
            file_->cd();
            ensureBranch(meta_tree_, "key", &meta_key_);
            ensureBranch(meta_tree_, "value", &meta_value_);

            if (!friend_tree_) {
                friend_tree_ = new TTree(kFriendTreeName, kFriendTreeTitle);
                friend_tree_->SetDirectory(file_.get());
            }
            ensureBranch(friend_tree_, "entry_id", &current_friend_.entry_id);
            ensureBranch(friend_tree_, "label", &current_friend_.label);
            ensureBranch(friend_tree_, "tree", &current_friend_.tree);
            ensureBranch(friend_tree_, "path", &current_friend_.path);
        }

        if (catalog_tree_) {
            next_entry_id_ = static_cast<UInt_t>(catalog_tree_->GetEntries());
        }
    }
}

HubCatalog::~HubCatalog() { finalize(); }

void HubCatalog::addEntry(const HubEntry &entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!catalog_tree_) {
        return;
    }

    current_entry_ = entry;
    if (current_entry_.entry_id == 0U) {
        current_entry_.entry_id = next_entry_id_++;
    } else {
        next_entry_id_ = std::max(next_entry_id_, static_cast<UInt_t>(current_entry_.entry_id + 1));
    }

    catalog_tree_->Fill();

    if (friend_tree_ && !current_entry_.friend_path.empty()) {
        current_friend_.entry_id = current_entry_.entry_id;
        current_friend_.label.clear();
        current_friend_.tree = current_entry_.friend_tree;
        current_friend_.path = current_entry_.friend_path;
        friend_tree_->Fill();
    }
}

void HubCatalog::addEntries(const std::vector<HubEntry> &entries) {
    for (const auto &entry : entries) {
        this->addEntry(entry);
    }
}

void HubCatalog::addFriend(const HubFriend &friend_entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!friend_tree_) {
        return;
    }

    current_friend_ = friend_entry;
    friend_tree_->Fill();
}

void HubCatalog::addFriends(const std::vector<HubFriend> &friend_entries) {
    for (const auto &entry : friend_entries) {
        this->addFriend(entry);
    }
}

void HubCatalog::writeDictionaries(const ProvenanceDicts &dicts) {
    nlohmann::json dict_json;
    dict_json["sample2id"] = dicts.sample2id;
    dict_json["beam2id"] = dicts.beam2id;
    dict_json["period2id"] = dicts.period2id;
    dict_json["stage2id"] = dicts.stage2id;
    dict_json["var2id"] = dicts.var2id;

    nlohmann::json origin_json = nlohmann::json::object();
    for (const auto &entry : dicts.origin2id) {
        origin_json[originToString(entry.first)] = entry.second;
    }
    dict_json["origin2id"] = origin_json;

    std::lock_guard<std::mutex> lock(mutex_);
    if (!meta_tree_) {
        return;
    }

    meta_key_ = "provenance_dicts";
    meta_value_ = dict_json.dump();
    meta_tree_->Fill();
}

void HubCatalog::writeSummary(double total_pot, long total_triggers, const std::string &base_directory,
                              const std::string &friend_tree_name) {
    nlohmann::json summary;
    summary["total_pot"] = total_pot;
    summary["total_triggers"] = total_triggers;
    summary["base_directory"] = base_directory;
    summary["friend_tree"] = friend_tree_name;

    std::lock_guard<std::mutex> lock(mutex_);
    if (!meta_tree_) {
        return;
    }

    meta_key_ = "summary";
    meta_value_ = summary.dump();
    meta_tree_->Fill();
}

void HubCatalog::finalize() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!file_ || finalized_) {
        return;
    }

    file_->cd();
    if (catalog_tree_) {
        catalog_tree_->Write("", TObject::kOverwrite);
    }
    if (meta_tree_) {
        meta_tree_->Write("", TObject::kOverwrite);
    }
    if (friend_tree_) {
        friend_tree_->Write("", TObject::kOverwrite);
    }
    file_->Write("", TObject::kOverwrite);
    file_->Close();
    finalized_ = true;
}

} // namespace proc
