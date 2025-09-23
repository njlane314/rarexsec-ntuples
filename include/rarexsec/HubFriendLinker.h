#ifndef RAREXSEC_HUB_FRIEND_LINKER_H
#define RAREXSEC_HUB_FRIEND_LINKER_H

#include <filesystem>
#include <optional>
#include <string>

namespace proc {

struct FriendLinkOptions {
    std::string hub_path;                                      ///< Path to the hub catalogue (.hub.root)
    std::optional<std::filesystem::path> friend_file;          ///< Single friend file to attach to all entries
    std::optional<std::filesystem::path> friend_directory;     ///< Base directory that mirrors hub shards
    bool mirror_structure = false;                             ///< Preserve shard subdirectories under friend_directory
    bool allow_missing = false;                                ///< Skip missing friend files instead of aborting
    bool make_relative = true;                                 ///< Store friend paths relative to the hub directory
    std::optional<std::string> friend_tree;                    ///< Optional override for the friend tree name
    std::string filename_suffix;                               ///< Suffix appended to the shard stem when building paths
    std::optional<std::string> filename_extension;             ///< Explicit extension for generated friend files
};

/// Update the hub catalogue to reference external friend files.
void linkFriendFiles(const FriendLinkOptions &options);

} // namespace proc

#endif
