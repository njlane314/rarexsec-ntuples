#include <rarexsec/HubDataFrame.h>

#include <rarexsec/LoggerUtils.h>

#include <algorithm>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <vector>

#include <ROOT/RDataFrame.hxx>
#include <TChain.h>

namespace {
constexpr const char *kCatalogTreeName = "entries";
constexpr const char *kMetaTreeName = "hub_meta";
}

namespace proc {

HubDataFrame::HubDataFrame(const std::string &hub_path)
    : hub_path_(hub_path), friend_tree_name_("meta") {
    try {
        ROOT::RDataFrame meta_df(kMetaTreeName, hub_path_);
        auto keys_result = meta_df.Take<std::string>("key");
        auto values_result = meta_df.Take<std::string>("value");
        const auto keys = keys_result.GetValue();
        const auto values = values_result.GetValue();

        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] == "summary") {
                try {
                    const auto summary = nlohmann::json::parse(values[i]);
                    if (summary.contains("base_directory")) {
                        base_directory_ = summary.at("base_directory").get<std::string>();
                    }
                    if (summary.contains("friend_tree")) {
                        friend_tree_name_ = summary.at("friend_tree").get<std::string>();
                    }
                } catch (const std::exception &ex) {
                    log::info("HubDataFrame", "[warning]", "Failed to parse hub summary metadata:", ex.what());
                }
            }
        }
    } catch (const std::exception &ex) {
        log::info("HubDataFrame", "[warning]", "Unable to load hub metadata:", ex.what());
    }
}

ROOT::RDF::RNode HubDataFrame::query(const std::string &beam,
                                      const std::string &period,
                                      const std::string &variation,
                                      const std::string &origin,
                                      const std::string &stage) {
    ROOT::RDataFrame catalog_df(kCatalogTreeName, hub_path_);
    auto filtered = catalog_df.Filter(
        [&](const std::string &cat_beam, const std::string &cat_period, const std::string &cat_variation,
            const std::string &cat_origin, const std::string &cat_stage) {
            if (cat_beam != beam) {
                return false;
            }
            if (cat_period != period) {
                return false;
            }
            if (!variation.empty() && cat_variation != variation) {
                return false;
            }
            if (!origin.empty() && cat_origin != origin) {
                return false;
            }
            if (!stage.empty() && cat_stage != stage) {
                return false;
            }
            return true;
        },
        {"beam", "period", "variation", "origin", "stage"});

    auto dataset_paths_result = filtered.Take<std::string>("dataset_path");
    auto dataset_trees_result = filtered.Take<std::string>("dataset_tree");
    auto friend_paths_result = filtered.Take<std::string>("friend_path");
    auto friend_trees_result = filtered.Take<std::string>("friend_tree");

    auto dataset_paths = dataset_paths_result.GetValue();
    auto dataset_trees = dataset_trees_result.GetValue();
    auto friend_paths = friend_paths_result.GetValue();
    auto friend_trees = friend_trees_result.GetValue();

    if (dataset_paths.empty()) {
        throw std::runtime_error("No hub entries matched the requested selection");
    }

    const std::string dataset_tree = dataset_trees.empty() ? std::string{"events"} : dataset_trees.front();
    const bool consistent_dataset_tree =
        std::all_of(dataset_trees.begin(), dataset_trees.end(),
                    [&](const std::string &name) { return name == dataset_tree; });
    if (!consistent_dataset_tree) {
        log::info("HubDataFrame", "[warning]", "Hub catalog lists mixed dataset tree names; using", dataset_tree);
    }

    std::string resolved_friend_tree = friend_tree_name_;
    if (!friend_trees.empty() && !friend_trees.front().empty()) {
        const bool consistent_friend_tree =
            std::all_of(friend_trees.begin(), friend_trees.end(),
                        [&](const std::string &name) { return name == friend_trees.front(); });
        if (!consistent_friend_tree) {
            log::info("HubDataFrame", "[warning]", "Hub catalog lists mixed friend tree names; using", friend_trees.front());
        }
        resolved_friend_tree = friend_trees.front();
    }

    current_chain_ = std::make_unique<TChain>(dataset_tree.c_str());
    friend_chain_ = std::make_unique<TChain>(resolved_friend_tree.c_str());

    const std::filesystem::path hub_dir = std::filesystem::absolute(std::filesystem::path(hub_path_)).parent_path();
    std::filesystem::path base_dir_path;
    if (!base_directory_.empty()) {
        base_dir_path = std::filesystem::path(base_directory_);
        if (base_dir_path.is_relative()) {
            base_dir_path = std::filesystem::absolute(hub_dir / base_dir_path);
        }
    }

    for (std::size_t i = 0; i < dataset_paths.size(); ++i) {
        std::filesystem::path dataset_path(dataset_paths[i]);
        if (dataset_path.is_relative()) {
            if (!base_dir_path.empty()) {
                dataset_path = base_dir_path / dataset_path;
            } else {
                dataset_path = hub_dir / dataset_path;
            }
        }
        current_chain_->Add(dataset_path.string().c_str());

        if (i < friend_paths.size()) {
            std::filesystem::path friend_path(friend_paths[i]);
            if (friend_path.is_relative()) {
                friend_path = hub_dir / friend_path;
            }
            friend_chain_->Add(friend_path.string().c_str());
        }
    }

    if (friend_chain_->GetListOfFiles()->GetEntries() > 0) {
        current_chain_->AddFriend(friend_chain_.get());
    } else {
        log::info("HubDataFrame", "[warning]", "No friend trees available for selection", beam, period, variation, origin,
                  stage);
    }

    log::info("HubDataFrame", "Loaded", dataset_paths.size(), "entries for", beam, period, variation, origin, stage);
    ROOT::RDataFrame df(*current_chain_);
    return df;
}

} // namespace proc
