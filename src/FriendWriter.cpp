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
    auto snapshot = df.Snapshot("meta", path.string(), columns, opt);
    snapshot.GetValue();

    return path;
}

std::filesystem::path FriendWriter::generateFriendPath(const std::string &sample_key,
                                                       const std::string &variation) const {
    std::ostringstream filename;
    filename << sample_key << "_" << variation << "_friend.root";
    return config_.output_dir / filename.str();
}

} // namespace proc
