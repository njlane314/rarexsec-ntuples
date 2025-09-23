#include <rarexsec/HubFriendLinker.h>

#include <rarexsec/HubDataFrame.h>
#include <rarexsec/LoggerUtils.h>

#include <ROOT/RDataFrame.hxx>
#include <TFile.h>
#include <TTree.h>

#include <nlohmann/json.hpp>

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

namespace proc {
namespace {

std::filesystem::path resolveAgainstHub(const std::filesystem::path &path, const std::filesystem::path &hub_dir) {
    if (path.empty()) {
        return path;
    }
    if (path.is_absolute()) {
        return path.lexically_normal();
    }
    return (hub_dir / path).lexically_normal();
}

std::string storedPath(const std::filesystem::path &absolute_path, const std::filesystem::path &hub_dir, bool make_relative) {
    std::filesystem::path normalised = absolute_path.is_absolute() ? absolute_path.lexically_normal()
                                                                   : std::filesystem::absolute(absolute_path).lexically_normal();
    if (!make_relative) {
        return normalised.generic_string();
    }

    std::error_code ec;
    auto relative = std::filesystem::relative(normalised, hub_dir, ec);
    if (!ec) {
        return relative.generic_string();
    }
    return normalised.generic_string();
}

std::string normalisedExtension(const std::optional<std::string> &extension_override,
                                const std::filesystem::path &reference_path) {
    if (!extension_override) {
        return reference_path.extension().string();
    }
    if (extension_override->empty()) {
        return std::string{};
    }
    if (extension_override->front() == '.') {
        return *extension_override;
    }
    return std::string{"."} + *extension_override;
}

std::filesystem::path buildFriendPath(const HubDataFrame::EntryInfo &entry, const FriendLinkOptions &options,
                                      const std::filesystem::path &base_dir) {
    std::filesystem::path dataset_path(entry.dataset_path);
    std::filesystem::path relative_component;
    if (options.mirror_structure) {
        relative_component = dataset_path;
    } else {
        relative_component = dataset_path.filename();
    }
    if (relative_component.empty()) {
        relative_component = dataset_path;
    }

    std::filesystem::path candidate = base_dir / relative_component;
    const std::string extension = normalisedExtension(options.filename_extension, candidate);
    std::string stem = candidate.stem().string();
    stem += options.filename_suffix;

    std::filesystem::path final_path;
    if (extension.empty()) {
        final_path = candidate.parent_path() / stem;
    } else {
        final_path = candidate.parent_path() / (stem + extension);
    }

    return final_path.lexically_normal();
}

void rewriteEntriesTree(const std::string &hub_path, const std::vector<std::string> &column_names,
                        ROOT::RDF::RNode updated_df) {
    std::filesystem::path temp_path(hub_path);
    temp_path += ".entries.tmp";

    ROOT::RDF::RSnapshotOptions snapshot_options;
    snapshot_options.fMode = "RECREATE";
    snapshot_options.fOverwriteIfExists = true;

    updated_df.Snapshot("entries", temp_path.string(), column_names, snapshot_options);

    TFile temp_file(temp_path.string().c_str(), "READ");
    if (temp_file.IsZombie()) {
        throw std::runtime_error("Failed to materialise updated entries tree at " + temp_path.string());
    }
    TTree *temp_entries = nullptr;
    temp_file.GetObject("entries", temp_entries);
    if (!temp_entries) {
        throw std::runtime_error("Temporary entries tree missing from " + temp_path.string());
    }

    TFile hub_file(hub_path.c_str(), "UPDATE");
    if (hub_file.IsZombie()) {
        throw std::runtime_error("Failed to open hub for update: " + hub_path);
    }

    hub_file.Delete("entries;*");
    std::unique_ptr<TTree> cloned(temp_entries->CloneTree(-1, "fast"));
    if (!cloned) {
        throw std::runtime_error("Unable to clone updated entries tree");
    }
    cloned->SetDirectory(&hub_file);
    cloned->Write("", TObject::kOverwrite);
    hub_file.Write("", TObject::kOverwrite);

    temp_file.Close();
    hub_file.Close();
    std::error_code remove_ec;
    std::filesystem::remove(temp_path, remove_ec);
    if (remove_ec) {
        log::info("HubFriendLinker", "[warning]", "Failed to remove temporary file", temp_path.string(), remove_ec.message());
    }
}

void rewriteMetaTree(const std::string &hub_path, std::vector<std::string> keys, std::vector<std::string> values) {
    TFile hub_file(hub_path.c_str(), "UPDATE");
    if (hub_file.IsZombie()) {
        throw std::runtime_error("Failed to open hub for metadata update: " + hub_path);
    }

    hub_file.Delete("hub_meta;*");

    TTree meta_tree("hub_meta", "Hub metadata");
    std::string key_buffer;
    std::string value_buffer;
    meta_tree.Branch("key", &key_buffer);
    meta_tree.Branch("value", &value_buffer);

    const auto entries = keys.size();
    for (std::size_t i = 0; i < entries; ++i) {
        key_buffer = keys[i];
        value_buffer = values[i];
        meta_tree.Fill();
    }

    hub_file.cd();
    meta_tree.Write("", TObject::kOverwrite);
    hub_file.Write("", TObject::kOverwrite);
    hub_file.Close();
}

} // namespace

void linkFriendFiles(const FriendLinkOptions &options) {
    if (options.hub_path.empty()) {
        throw std::invalid_argument("Hub path must not be empty");
    }
    if (!options.friend_file && !options.friend_directory) {
        throw std::invalid_argument("Specify either friend_file or friend_directory in FriendLinkOptions");
    }
    if (options.friend_file && options.friend_directory) {
        throw std::invalid_argument("Provide only one of friend_file or friend_directory");
    }

    std::filesystem::path hub_path(options.hub_path);
    if (!std::filesystem::exists(hub_path)) {
        throw std::runtime_error("Hub file does not exist: " + options.hub_path);
    }

    const std::filesystem::path hub_dir = hub_path.parent_path().lexically_normal();

    HubDataFrame hub(options.hub_path);
    const auto &entries = hub.entries();
    if (entries.empty()) {
        log::info("HubFriendLinker", "No entries found in hub", options.hub_path);
        return;
    }

    std::unordered_map<UInt_t, std::string> path_map;
    std::unordered_map<UInt_t, std::string> tree_map;
    path_map.reserve(entries.size());
    tree_map.reserve(entries.size());

    std::filesystem::path resolved_friend_file;
    bool single_available = false;
    if (options.friend_file) {
        resolved_friend_file = resolveAgainstHub(*options.friend_file, hub_dir);
        std::error_code exists_ec;
        single_available = std::filesystem::exists(resolved_friend_file, exists_ec);
        if (exists_ec) {
            throw std::runtime_error("Failed to check existence of friend file " + resolved_friend_file.string() + ": " + exists_ec.message());
        }
        if (!single_available && !options.allow_missing) {
            throw std::runtime_error("Friend file not found: " + resolved_friend_file.string());
        }
    }

    std::filesystem::path resolved_friend_dir;
    if (options.friend_directory) {
        resolved_friend_dir = resolveAgainstHub(*options.friend_directory, hub_dir);
        if (!std::filesystem::exists(resolved_friend_dir)) {
            throw std::runtime_error("Friend directory does not exist: " + resolved_friend_dir.string());
        }
    }

    std::size_t missing_files = 0U;
    std::size_t updated_paths = 0U;
    std::size_t updated_trees = 0U;

    for (const auto &entry : entries) {
        std::string new_path = entry.friend_path;
        bool updated = false;
        bool friend_present = true;

        if (options.friend_file) {
            if (single_available) {
                const std::string stored = storedPath(resolved_friend_file, hub_dir, options.make_relative);
                if (stored != entry.friend_path) {
                    updated = true;
                    new_path = stored;
                }
            } else {
                friend_present = false;
                ++missing_files;
            }
        } else if (options.friend_directory) {
            std::filesystem::path candidate = buildFriendPath(entry, options, resolved_friend_dir);
            std::error_code exists_ec;
            const bool exists = std::filesystem::exists(candidate, exists_ec);
            if (exists_ec) {
                throw std::runtime_error("Failed to check existence of friend shard " + candidate.string() + ": " + exists_ec.message());
            }
            if (!exists) {
                friend_present = false;
                ++missing_files;
                if (!options.allow_missing) {
                    throw std::runtime_error("Friend shard missing for entry " + entry.sample_key + " (" + candidate.string() + ")");
                }
            } else {
                const std::string stored = storedPath(candidate, hub_dir, options.make_relative);
                if (stored != entry.friend_path) {
                    updated = true;
                    new_path = stored;
                }
            }
        }

        if (!updated && !friend_present && options.allow_missing) {
            new_path = entry.friend_path;
        } else if (!friend_present && !options.allow_missing) {
            // Defensive: should already have thrown above.
            throw std::runtime_error("Required friend file missing for entry " + entry.sample_key);
        }

        if (updated) {
            ++updated_paths;
        }
        path_map.emplace(entry.entry_id, new_path);

        std::string new_tree = entry.friend_tree;
        if (options.friend_tree && friend_present) {
            new_tree = *options.friend_tree;
        }
        if (new_tree != entry.friend_tree) {
            ++updated_trees;
        }
        tree_map.emplace(entry.entry_id, new_tree);
    }

    auto path_map_ptr = std::make_shared<std::unordered_map<UInt_t, std::string>>(std::move(path_map));
    auto tree_map_ptr = std::make_shared<std::unordered_map<UInt_t, std::string>>(std::move(tree_map));

    ROOT::RDF::RDataFrame entries_df("entries", options.hub_path);
    auto updated_df = entries_df.Redefine(
        "friend_path",
        [path_map_ptr](UInt_t entry_id) {
            const auto it = path_map_ptr->find(entry_id);
            if (it == path_map_ptr->end()) {
                throw std::runtime_error("Missing friend_path mapping for entry id " + std::to_string(entry_id));
            }
            return it->second;
        },
        {"entry_id"});

    updated_df = updated_df.Redefine(
        "friend_tree",
        [tree_map_ptr](UInt_t entry_id) {
            const auto it = tree_map_ptr->find(entry_id);
            if (it == tree_map_ptr->end()) {
                throw std::runtime_error("Missing friend_tree mapping for entry id " + std::to_string(entry_id));
            }
            return it->second;
        },
        {"entry_id"});

    const auto column_names_vec = updated_df.GetColumnNames();
    std::vector<std::string> column_names(column_names_vec.begin(), column_names_vec.end());

    rewriteEntriesTree(options.hub_path, column_names, updated_df);

    if (options.friend_tree) {
        ROOT::RDF::RDataFrame meta_df("hub_meta", options.hub_path);
        auto keys_result = meta_df.Take<std::string>("key");
        auto values_result = meta_df.Take<std::string>("value");
        auto keys = keys_result.GetValue();
        auto values = values_result.GetValue();

        bool summary_updated = false;
        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] == "summary") {
                try {
                    auto summary = nlohmann::json::parse(values[i]);
                    summary["friend_tree"] = *options.friend_tree;
                    values[i] = summary.dump();
                    summary_updated = true;
                } catch (const std::exception &ex) {
                    log::info("HubFriendLinker", "[warning]", "Failed to update summary metadata:", ex.what());
                }
            }
        }

        if (summary_updated) {
            rewriteMetaTree(options.hub_path, std::move(keys), std::move(values));
        } else {
            log::info("HubFriendLinker", "[warning]", "Hub metadata did not contain a summary entry; friend tree name was not updated");
        }
    }

    log::info("HubFriendLinker", "Updated", updated_paths, "hub entries with new friend paths");
    if (options.friend_tree) {
        log::info("HubFriendLinker", "Set friend tree to", *options.friend_tree, "for", updated_trees, "entries");
    }
    if (missing_files > 0U) {
        if (options.allow_missing) {
            log::info("HubFriendLinker", "[warning]", missing_files,
                      "friend files were missing; existing catalog entries were left unchanged");
        } else {
            log::info("HubFriendLinker", "[warning]", missing_files, "friend files missing");
        }
    }
}

} // namespace proc
