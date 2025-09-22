#include <rarexsec/HubDataFrame.h>

#include <rarexsec/LoggerUtils.h>

#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <vector>

namespace {
constexpr const char *kCatalogTreeName = "shards";
}

namespace proc {

HubDataFrame::HubDataFrame(const std::string &hub_path) : hub_path_(hub_path) {}

ROOT::RDF::RNode HubDataFrame::query(const std::string &beam,
                                      const std::string &period,
                                      const std::string &variation,
                                      const std::string &origin,
                                      const std::string &stage) {
    ROOT::RDF::RDataFrame catalog_df(kCatalogTreeName, hub_path_);
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

    auto shard_paths_result = filtered.Take<std::string>("shard_path");
    auto tree_names_result = filtered.Take<std::string>("tree_name");

    auto shard_paths = shard_paths_result.GetValue();
    auto tree_names = tree_names_result.GetValue();

    if (shard_paths.empty()) {
        throw std::runtime_error("No shards matched the requested selection");
    }

    const std::string tree_name = tree_names.empty() ? std::string{"events"} : tree_names.front();
    const bool consistent_tree = std::all_of(tree_names.begin(), tree_names.end(),
                                             [&](const std::string &name) { return name == tree_name; });
    if (!consistent_tree) {
        log::info("HubDataFrame", "[warning]", "Shard catalog lists mixed tree names; using", tree_name);
    }

    current_chain_ = std::make_unique<TChain>(tree_name.c_str());
    const std::filesystem::path hub_dir = std::filesystem::absolute(std::filesystem::path(hub_path_)).parent_path();

    for (const auto &path : shard_paths) {
        std::filesystem::path shard_path(path);
        if (shard_path.is_relative()) {
            shard_path = hub_dir / shard_path;
        }
        current_chain_->Add(shard_path.string().c_str());
    }

    log::info("HubDataFrame", "Loaded", shard_paths.size(), "shards for", beam, period, variation, origin, stage);
    ROOT::RDF::RDataFrame df(*current_chain_);
    return df;
}

} // namespace proc
