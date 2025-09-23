#include <rarexsec/HubDataFrame.h>

#include <rarexsec/LoggerUtils.h>

#include <algorithm>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <ROOT/RDataFrame.hxx>
#include <TChain.h>

namespace {
constexpr const char *kCatalogTreeName = "entries";
constexpr const char *kMetaTreeName = "hub_meta";

bool matchesValue(const std::optional<std::string> &selector, const std::string &value) {
    return !selector || value == *selector;
}

template <typename MapT>
void parseNumericMap(const nlohmann::json &source, const char *key, MapT &target) {
    if (!source.contains(key)) {
        return;
    }
    const auto &object = source.at(key);
    if (!object.is_object()) {
        return;
    }
    for (auto it = object.begin(); it != object.end(); ++it) {
        using ValueType = typename MapT::mapped_type;
        try {
            target[it.key()] = it.value().template get<ValueType>();
        } catch (const std::exception &ex) {
            proc::log::info("HubDataFrame", "[warning]", "Failed to parse provenance entry for", key, it.key(),
                            ":", ex.what());
        }
    }
}

} // namespace

namespace proc {

HubDataFrame::Selection::Selection(HubDataFrame &owner)
    : owner_(owner), variation_(std::string{"nominal"}) {}

namespace {
std::optional<std::string> normaliseSelectorValue(const std::string &value) {
    if (value.empty()) {
        return std::nullopt;
    }
    return value;
}
} // namespace

Selection &HubDataFrame::Selection::sample(const std::string &value) {
    sample_ = normaliseSelectorValue(value);
    return *this;
}

Selection &HubDataFrame::Selection::beam(const std::string &value) {
    beam_ = normaliseSelectorValue(value);
    return *this;
}

Selection &HubDataFrame::Selection::period(const std::string &value) {
    period_ = normaliseSelectorValue(value);
    return *this;
}

Selection &HubDataFrame::Selection::variation(const std::string &value) {
    variation_ = normaliseSelectorValue(value);
    return *this;
}

Selection &HubDataFrame::Selection::origin(const std::string &value) {
    origin_ = normaliseSelectorValue(value);
    return *this;
}

Selection &HubDataFrame::Selection::stage(const std::string &value) {
    stage_ = normaliseSelectorValue(value);
    return *this;
}

Selection &HubDataFrame::Selection::clearSample() {
    sample_.reset();
    return *this;
}

Selection &HubDataFrame::Selection::clearBeam() {
    beam_.reset();
    return *this;
}

Selection &HubDataFrame::Selection::clearPeriod() {
    period_.reset();
    return *this;
}

Selection &HubDataFrame::Selection::clearVariation() {
    variation_.reset();
    return *this;
}

Selection &HubDataFrame::Selection::clearOrigin() {
    origin_.reset();
    return *this;
}

Selection &HubDataFrame::Selection::clearStage() {
    stage_.reset();
    return *this;
}

std::vector<const HubDataFrame::CatalogEntry *> HubDataFrame::Selection::entries() const {
    return owner_.resolveEntries(sample_, beam_, period_, variation_, origin_, stage_);
}

ROOT::RDF::RNode HubDataFrame::Selection::load() {
    return owner_.loadSelection(sample_, beam_, period_, variation_, origin_, stage_);
}

HubDataFrame::HubDataFrame(const std::string &hub_path)
    : hub_path_(hub_path),
      hub_directory_(std::filesystem::absolute(std::filesystem::path(hub_path)).parent_path()) {
    this->loadMetadata();
    this->loadCatalog();
}

HubDataFrame::Selection HubDataFrame::select() { return Selection(*this); }

ROOT::RDF::RNode HubDataFrame::query(const std::string &beam, const std::string &period,
                                     const std::string &variation, const std::string &origin,
                                     const std::string &stage) {
    std::optional<std::string> beam_selector{beam};
    std::optional<std::string> period_selector{period};
    std::optional<std::string> variation_selector;
    std::optional<std::string> origin_selector;
    std::optional<std::string> stage_selector;

    if (!variation.empty()) {
        variation_selector = variation;
    }
    if (!origin.empty()) {
        origin_selector = origin;
    }
    if (!stage.empty()) {
        stage_selector = stage;
    }

    return loadSelection(std::nullopt, beam_selector, period_selector, variation_selector, origin_selector,
                         stage_selector);
}

std::vector<const HubDataFrame::CatalogEntry *> HubDataFrame::resolveEntries(
    const std::optional<std::string> &sample, const std::optional<std::string> &beam,
    const std::optional<std::string> &period, const std::optional<std::string> &variation,
    const std::optional<std::string> &origin, const std::optional<std::string> &stage) const {
    std::vector<const CatalogEntry *> matches;
    matches.reserve(entries_.size());
    for (const auto &entry : entries_) {
        if (!matchesValue(sample, entry.sample_key)) {
            continue;
        }
        if (!matchesValue(beam, entry.beam)) {
            continue;
        }
        if (!matchesValue(period, entry.period)) {
            continue;
        }
        if (!matchesValue(variation, entry.variation)) {
            continue;
        }
        if (!matchesValue(origin, entry.origin)) {
            continue;
        }
        if (!matchesValue(stage, entry.stage)) {
            continue;
        }
        matches.push_back(&entry);
    }
    return matches;
}

ROOT::RDF::RNode HubDataFrame::loadSelection(const std::optional<std::string> &sample,
                                             const std::optional<std::string> &beam,
                                             const std::optional<std::string> &period,
                                             const std::optional<std::string> &variation,
                                             const std::optional<std::string> &origin,
                                             const std::optional<std::string> &stage) {
    auto matches = resolveEntries(sample, beam, period, variation, origin, stage);
    if (matches.empty()) {
        throw std::runtime_error("No hub entries matched the requested selection");
    }
    return buildDataFrame(matches);
}

ROOT::RDF::RNode HubDataFrame::buildDataFrame(const std::vector<const CatalogEntry *> &entries) {
    if (entries.empty()) {
        throw std::runtime_error("No hub entries matched the requested selection");
    }

    const CatalogEntry &first = *entries.front();

    const std::string dataset_tree =
        first.dataset_tree.empty() ? std::string{"events"} : first.dataset_tree;
    const bool consistent_dataset_tree =
        std::all_of(entries.begin(), entries.end(), [&](const CatalogEntry *entry) {
            return entry->dataset_tree.empty() || entry->dataset_tree == dataset_tree;
        });
    if (!consistent_dataset_tree) {
        log::info("HubDataFrame", "[warning]", "Hub catalog lists mixed dataset tree names; using", dataset_tree);
    }

    std::string resolved_friend_tree = summary_.friend_tree;
    bool friend_tree_seen = false;
    for (const auto *entry : entries) {
        if (entry->friend_tree.empty()) {
            continue;
        }
        if (!friend_tree_seen) {
            resolved_friend_tree = entry->friend_tree;
            friend_tree_seen = true;
            continue;
        }
        if (entry->friend_tree != resolved_friend_tree) {
            log::info("HubDataFrame", "[warning]", "Hub catalog lists mixed friend tree names; using",
                      resolved_friend_tree);
            break;
        }
    }

    current_chain_ = std::make_unique<TChain>(dataset_tree.c_str());
    friend_chain_ = std::make_unique<TChain>(resolved_friend_tree.c_str());

    for (const auto *entry : entries) {
        const auto dataset_path = resolveDatasetPath(*entry);
        current_chain_->Add(dataset_path.string().c_str());

        if (!entry->friend_path.empty()) {
            const auto friend_path = resolveFriendPath(*entry);
            friend_chain_->Add(friend_path.string().c_str());
        }
    }

    if (friend_chain_->GetListOfFiles()->GetEntries() > 0) {
        current_chain_->AddFriend(friend_chain_.get());
    } else {
        log::info("HubDataFrame", "[warning]", "No friend trees available for selection", first.beam, first.period,
                  first.variation, first.origin, first.stage);
    }

    log::info("HubDataFrame", "Loaded", entries.size(), "entries for", first.beam, first.period, first.variation,
              first.origin, first.stage);
    return ROOT::RDataFrame(*current_chain_);
}

std::filesystem::path HubDataFrame::resolveDatasetPath(const CatalogEntry &entry) const {
    std::filesystem::path dataset_path(entry.dataset_path);
    if (dataset_path.is_absolute()) {
        if (base_directory_override_ && !summary_.resolved_base_directory.empty()) {
            std::error_code ec;
            auto relative = std::filesystem::relative(dataset_path, summary_.resolved_base_directory, ec);
            if (!ec) {
                return *base_directory_override_ / relative;
            }
            log::info("HubDataFrame", "[warning]", "Unable to rebase dataset path", dataset_path.string(),
                      "using override base directory");
        }
        return dataset_path;
    }

    if (base_directory_override_) {
        return *base_directory_override_ / dataset_path;
    }

    if (!summary_.resolved_base_directory.empty()) {
        return summary_.resolved_base_directory / dataset_path;
    }

    return hub_directory_ / dataset_path;
}

std::filesystem::path HubDataFrame::resolveFriendPath(const CatalogEntry &entry) const {
    if (entry.friend_path.empty()) {
        return {};
    }
    std::filesystem::path friend_path(entry.friend_path);
    if (friend_path.is_absolute()) {
        return friend_path;
    }
    return hub_directory_ / friend_path;
}

void HubDataFrame::setBaseDirectoryOverride(const std::filesystem::path &path) {
    if (path.empty()) {
        base_directory_override_.reset();
        return;
    }
    base_directory_override_ = path.is_absolute() ? path : std::filesystem::absolute(path);
}

void HubDataFrame::clearBaseDirectoryOverride() { base_directory_override_.reset(); }

std::filesystem::path HubDataFrame::resolvedBaseDirectory() const {
    if (base_directory_override_) {
        return *base_directory_override_;
    }
    if (!summary_.resolved_base_directory.empty()) {
        return summary_.resolved_base_directory;
    }
    return hub_directory_;
}

void HubDataFrame::loadMetadata() {
    summary_.friend_tree = "meta";
    try {
        ROOT::RDataFrame meta_df(kMetaTreeName, hub_path_);
        auto keys = meta_df.Take<std::string>("key").GetValue();
        auto values = meta_df.Take<std::string>("value").GetValue();

        for (std::size_t i = 0; i < keys.size(); ++i) {
            const auto &key = keys[i];
            const auto &value = values[i];
            if (key == "summary") {
                try {
                    const auto summary_json = nlohmann::json::parse(value);
                    summary_.total_pot = summary_json.value("total_pot", 0.0);
                    summary_.total_triggers = summary_json.value("total_triggers", 0L);
                    if (summary_json.contains("base_directory")) {
                        summary_.base_directory = summary_json.at("base_directory").get<std::string>();
                    }
                    if (summary_json.contains("friend_tree")) {
                        summary_.friend_tree = summary_json.at("friend_tree").get<std::string>();
                    }
                } catch (const std::exception &ex) {
                    log::info("HubDataFrame", "[warning]", "Failed to parse hub summary metadata:", ex.what());
                }
            } else if (key == "provenance_dicts") {
                try {
                    const auto dict_json = nlohmann::json::parse(value);
                    parseNumericMap(dict_json, "sample2id", provenance_dicts_.sample_ids);
                    parseNumericMap(dict_json, "beam2id", provenance_dicts_.beam_ids);
                    parseNumericMap(dict_json, "period2id", provenance_dicts_.period_ids);
                    parseNumericMap(dict_json, "stage2id", provenance_dicts_.stage_ids);
                    parseNumericMap(dict_json, "var2id", provenance_dicts_.variation_ids);
                    parseNumericMap(dict_json, "origin2id", provenance_dicts_.origin_ids);
                } catch (const std::exception &ex) {
                    log::info("HubDataFrame", "[warning]", "Failed to parse provenance dictionaries:", ex.what());
                }
            }
        }
    } catch (const std::exception &ex) {
        log::info("HubDataFrame", "[warning]", "Unable to load hub metadata:", ex.what());
    }

    if (summary_.friend_tree.empty()) {
        summary_.friend_tree = "meta";
    }

    if (!summary_.base_directory.empty()) {
        std::filesystem::path base_dir(summary_.base_directory);
        if (base_dir.is_relative()) {
            summary_.resolved_base_directory = std::filesystem::absolute(hub_directory_ / base_dir);
        } else {
            summary_.resolved_base_directory = base_dir;
        }
    } else {
        summary_.resolved_base_directory.clear();
    }
}

void HubDataFrame::loadCatalog() {
    try {
        ROOT::RDataFrame catalog_df(kCatalogTreeName, hub_path_);
        auto entry_ids = catalog_df.Take<UInt_t>("entry_id").GetValue();
        auto sample_ids = catalog_df.Take<UInt_t>("sample_id").GetValue();
        auto beam_ids = catalog_df.Take<UShort_t>("beam_id").GetValue();
        auto period_ids = catalog_df.Take<UShort_t>("period_id").GetValue();
        auto variation_ids = catalog_df.Take<UShort_t>("variation_id").GetValue();
        auto origin_ids = catalog_df.Take<UChar_t>("origin_id").GetValue();
        auto dataset_paths = catalog_df.Take<std::string>("dataset_path").GetValue();
        auto dataset_trees = catalog_df.Take<std::string>("dataset_tree").GetValue();
        auto friend_paths = catalog_df.Take<std::string>("friend_path").GetValue();
        auto friend_trees = catalog_df.Take<std::string>("friend_tree").GetValue();
        auto n_events = catalog_df.Take<ULong64_t>("n_events").GetValue();
        auto first_uid = catalog_df.Take<ULong64_t>("first_event_uid").GetValue();
        auto last_uid = catalog_df.Take<ULong64_t>("last_event_uid").GetValue();
        auto sum_weights = catalog_df.Take<Double_t>("sum_weights").GetValue();
        auto pot = catalog_df.Take<Double_t>("pot").GetValue();
        auto triggers = catalog_df.Take<Long64_t>("triggers").GetValue();
        auto sample_keys = catalog_df.Take<std::string>("sample_key").GetValue();
        auto beams = catalog_df.Take<std::string>("beam").GetValue();
        auto periods = catalog_df.Take<std::string>("period").GetValue();
        auto variations = catalog_df.Take<std::string>("variation").GetValue();
        auto origins = catalog_df.Take<std::string>("origin").GetValue();
        auto stages = catalog_df.Take<std::string>("stage").GetValue();

        const std::size_t count = dataset_paths.size();
        entries_.clear();
        entries_.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            CatalogEntry entry;
            entry.entry_id = (i < entry_ids.size()) ? static_cast<std::uint32_t>(entry_ids[i]) : static_cast<std::uint32_t>(i);
            entry.sample_id = (i < sample_ids.size()) ? static_cast<std::uint32_t>(sample_ids[i]) : 0U;
            entry.beam_id = (i < beam_ids.size()) ? static_cast<std::uint16_t>(beam_ids[i]) : 0U;
            entry.period_id = (i < period_ids.size()) ? static_cast<std::uint16_t>(period_ids[i]) : 0U;
            entry.variation_id = (i < variation_ids.size()) ? static_cast<std::uint16_t>(variation_ids[i]) : 0U;
            entry.origin_id = (i < origin_ids.size()) ? static_cast<std::uint8_t>(origin_ids[i]) : 0U;
            entry.dataset_path = dataset_paths[i];
            entry.dataset_tree = (i < dataset_trees.size()) ? dataset_trees[i] : std::string{};
            entry.friend_path = (i < friend_paths.size()) ? friend_paths[i] : std::string{};
            entry.friend_tree = (i < friend_trees.size() && !friend_trees[i].empty()) ? friend_trees[i] : summary_.friend_tree;
            entry.n_events = (i < n_events.size()) ? static_cast<std::uint64_t>(n_events[i]) : 0ULL;
            entry.first_event_uid = (i < first_uid.size()) ? static_cast<std::uint64_t>(first_uid[i]) : 0ULL;
            entry.last_event_uid = (i < last_uid.size()) ? static_cast<std::uint64_t>(last_uid[i]) : 0ULL;
            entry.sum_weights = (i < sum_weights.size()) ? static_cast<double>(sum_weights[i]) : 0.0;
            entry.pot = (i < pot.size()) ? static_cast<double>(pot[i]) : 0.0;
            entry.triggers = (i < triggers.size()) ? static_cast<long>(triggers[i]) : 0L;
            entry.sample_key = (i < sample_keys.size()) ? sample_keys[i] : std::string{};
            entry.beam = (i < beams.size()) ? beams[i] : std::string{};
            entry.period = (i < periods.size()) ? periods[i] : std::string{};
            entry.variation = (i < variations.size()) ? variations[i] : std::string{};
            entry.origin = (i < origins.size()) ? origins[i] : std::string{};
            entry.stage = (i < stages.size()) ? stages[i] : std::string{};

            entries_.push_back(std::move(entry));
        }
    } catch (const std::exception &ex) {
        log::info("HubDataFrame", "[warning]", "Unable to load hub catalogue:", ex.what());
        entries_.clear();
    }
}

std::vector<std::string> HubDataFrame::beams() const {
    std::set<std::string> unique;
    for (const auto &entry : entries_) {
        unique.insert(entry.beam);
    }
    return {unique.begin(), unique.end()};
}

std::vector<std::string> HubDataFrame::periods(const std::optional<std::string> &beam) const {
    std::set<std::string> unique;
    for (const auto &entry : entries_) {
        if (beam && entry.beam != *beam) {
            continue;
        }
        unique.insert(entry.period);
    }
    return {unique.begin(), unique.end()};
}

std::vector<std::string> HubDataFrame::origins(const std::optional<std::string> &beam,
                                               const std::optional<std::string> &period,
                                               const std::optional<std::string> &stage) const {
    std::set<std::string> unique;
    for (const auto &entry : entries_) {
        if (beam && entry.beam != *beam) {
            continue;
        }
        if (period && entry.period != *period) {
            continue;
        }
        if (stage && entry.stage != *stage) {
            continue;
        }
        unique.insert(entry.origin);
    }
    return {unique.begin(), unique.end()};
}

std::vector<std::string> HubDataFrame::stages(const std::optional<std::string> &beam,
                                              const std::optional<std::string> &period) const {
    std::set<std::string> unique;
    for (const auto &entry : entries_) {
        if (beam && entry.beam != *beam) {
            continue;
        }
        if (period && entry.period != *period) {
            continue;
        }
        unique.insert(entry.stage);
    }
    return {unique.begin(), unique.end()};
}

std::vector<std::string> HubDataFrame::variations(const std::optional<std::string> &beam,
                                                  const std::optional<std::string> &period,
                                                  const std::optional<std::string> &origin,
                                                  const std::optional<std::string> &stage) const {
    std::set<std::string> unique;
    for (const auto &entry : entries_) {
        if (beam && entry.beam != *beam) {
            continue;
        }
        if (period && entry.period != *period) {
            continue;
        }
        if (origin && entry.origin != *origin) {
            continue;
        }
        if (stage && entry.stage != *stage) {
            continue;
        }
        unique.insert(entry.variation);
    }
    return {unique.begin(), unique.end()};
}

std::vector<std::string> HubDataFrame::sampleKeys(const std::optional<std::string> &beam,
                                                  const std::optional<std::string> &period,
                                                  const std::optional<std::string> &stage,
                                                  const std::optional<std::string> &variation) const {
    std::set<std::string> unique;
    for (const auto &entry : entries_) {
        if (beam && entry.beam != *beam) {
            continue;
        }
        if (period && entry.period != *period) {
            continue;
        }
        if (stage && entry.stage != *stage) {
            continue;
        }
        if (variation && entry.variation != *variation) {
            continue;
        }
        unique.insert(entry.sample_key);
    }
    return {unique.begin(), unique.end()};
}

} // namespace proc

