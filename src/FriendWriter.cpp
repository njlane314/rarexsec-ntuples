#include <rarexsec/FriendWriter.h>

#include <rarexsec/LoggerUtils.h>

#include <filesystem>
#include <iomanip>
#include <sstream>
#include <system_error>

namespace proc {

FriendWriter::FriendWriter(const FriendConfig &config) : config_(config) {
    std::error_code ec;
    std::filesystem::create_directories(config_.output_dir, ec);
    if (ec) {
        log::info("FriendWriter", "[warning]", "Failed to ensure friend output directory", config_.output_dir.string(),
                  ":", ec.message());
    }
}

std::filesystem::path FriendWriter::writeFriend(ROOT::RDF::RNode df,
                                                const std::string &sample_key,
                                                const std::string &variation,
                                                const std::vector<std::string> &columns) const {
    ROOT::RDF::RSnapshotOptions opt;
    opt.fCompressionAlgorithm = config_.compression_algo;
    opt.fCompressionLevel = config_.compression_level;
    opt.fAutoFlush = -30 * 1024 * 1024;
    opt.fSplitLevel = 0;
    opt.fOverwriteIfExists = true;

    auto path = generateFriendPath(sample_key, variation);
    return writeFriendToPath(df, path, columns);
}

std::filesystem::path FriendWriter::generateFriendPath(const std::string &sample_key,
                                                       const std::string &variation) const {
    std::ostringstream filename;
    filename << sample_key << "_" << variation << "_friend.root";
    return config_.output_dir / filename.str();
}

std::filesystem::path FriendWriter::writeFriendToPath(ROOT::RDF::RNode df,
                                                      const std::filesystem::path &path,
                                                      const std::vector<std::string> &columns) const {
    ROOT::RDF::RSnapshotOptions opt;
    opt.fCompressionAlgorithm = config_.compression_algo;
    opt.fCompressionLevel = config_.compression_level;
    opt.fAutoFlush = -30 * 1024 * 1024;
    opt.fSplitLevel = 0;
    opt.fOverwriteIfExists = true;

    std::filesystem::path resolved = path;
    const auto parent = resolved.parent_path();
    if (!parent.empty()) {
        std::error_code dir_ec;
        std::filesystem::create_directories(parent, dir_ec);
        if (dir_ec) {
            log::info("FriendWriter", "[warning]", "Failed to ensure friend parent directory", parent.string(),
                      ":", dir_ec.message());
        }
    }

    auto snapshot = df.Snapshot(config_.tree_name, resolved.string(), columns, opt);
    snapshot.GetValue();

    return resolved;
}

} // namespace proc
