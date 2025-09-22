#include <rarexsec/HubCatalog.h>

#include <rarexsec/SampleTypes.h>
#include <rarexsec/SnapshotPipelineBuilder.h>

#include "ROOT/RDataFrame.hxx"
#include "TObject.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace proc {
namespace {
constexpr const char *kCatalogTreeName = "shards";
constexpr const char *kMetaTreeName = "hub_meta";
constexpr const char *kCatalogTreeTitle = "Shard catalog";
constexpr const char *kMetaTreeTitle = "Hub metadata";
} // namespace

HubCatalog::HubCatalog(const std::string &hub_path, bool create)
    : catalog_tree_(nullptr),
      meta_tree_(nullptr),
      next_shard_id_(0U),
      finalized_(false) {
    const char *mode = create ? "RECREATE" : "READ";
    file_.reset(TFile::Open(hub_path.c_str(), mode));
    if (!file_ || file_->IsZombie()) {
        throw std::runtime_error("Failed to open hub catalog file: " + hub_path);
    }

    if (create) {
        file_->cd();
        catalog_tree_ = new TTree(kCatalogTreeName, kCatalogTreeTitle);
        catalog_tree_->SetDirectory(file_.get());
        catalog_tree_->Branch("shard_id", &current_entry_.shard_id);
        catalog_tree_->Branch("sample_id", &current_entry_.sample_id);
        catalog_tree_->Branch("beam_id", &current_entry_.beam_id);
        catalog_tree_->Branch("period_id", &current_entry_.period_id);
        catalog_tree_->Branch("variation_id", &current_entry_.variation_id);
        catalog_tree_->Branch("origin_id", &current_entry_.origin_id);
        catalog_tree_->Branch("shard_path", &current_entry_.shard_path);
        catalog_tree_->Branch("tree_name", &current_entry_.tree_name);
        catalog_tree_->Branch("n_events", &current_entry_.n_events);
        catalog_tree_->Branch("first_event_uid", &current_entry_.first_event_uid);
        catalog_tree_->Branch("last_event_uid", &current_entry_.last_event_uid);
        catalog_tree_->Branch("sum_weights", &current_entry_.sum_weights);
        catalog_tree_->Branch("pot", &current_entry_.pot);
        catalog_tree_->Branch("triggers", &current_entry_.triggers);
        catalog_tree_->Branch("sample_key", &current_entry_.sample_key);
        catalog_tree_->Branch("beam", &current_entry_.beam);
        catalog_tree_->Branch("period", &current_entry_.period);
        catalog_tree_->Branch("variation", &current_entry_.variation);
        catalog_tree_->Branch("origin", &current_entry_.origin);
        catalog_tree_->Branch("stage", &current_entry_.stage);

        meta_tree_ = new TTree(kMetaTreeName, kMetaTreeTitle);
        meta_tree_->SetDirectory(file_.get());
        meta_tree_->Branch("key", &meta_key_);
        meta_tree_->Branch("value", &meta_value_);

        meta_key_ = "hub_version";
        meta_value_ = "1";
        meta_tree_->Fill();
    } else {
        catalog_tree_ = dynamic_cast<TTree *>(file_->Get(kCatalogTreeName));
        meta_tree_ = dynamic_cast<TTree *>(file_->Get(kMetaTreeName));
        if (!catalog_tree_) {
            throw std::runtime_error("Hub catalog is missing the shard tree");
        }
    }
}

HubCatalog::~HubCatalog() { finalize(); }

void HubCatalog::addShardEntry(const ShardEntry &entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!catalog_tree_) {
        return;
    }

    current_entry_ = entry;
    if (current_entry_.shard_id == 0U) {
        current_entry_.shard_id = next_shard_id_++;
    } else {
        next_shard_id_ = std::max(next_shard_id_, static_cast<UInt_t>(current_entry_.shard_id + 1));
    }

    catalog_tree_->Fill();
}

void HubCatalog::addShardEntries(const std::vector<ShardEntry> &entries) {
    for (const auto &entry : entries) {
        this->addShardEntry(entry);
    }
}

std::vector<std::string> HubCatalog::queryShardPaths(const std::string &selection) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> paths;
    if (!catalog_tree_) {
        return paths;
    }

    ROOT::RDF::RDataFrame df(*catalog_tree_);
    auto node = df;
    if (!selection.empty()) {
        node = node.Filter(selection);
    }

    auto take_paths = node.Take<std::string>("shard_path");
    paths = *take_paths;
    return paths;
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

void HubCatalog::writeSummary(double total_pot, long total_triggers) {
    nlohmann::json summary;
    summary["total_pot"] = total_pot;
    summary["total_triggers"] = total_triggers;

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
    file_->Write("", TObject::kOverwrite);
    file_->Close();
    finalized_ = true;
}

} // namespace proc
