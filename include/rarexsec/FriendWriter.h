#ifndef FRIEND_WRITER_H
#define FRIEND_WRITER_H

#include <filesystem>
#include <string>
#include <vector>

#include <Compression.h>

#include "ROOT/RDataFrame.hxx"

namespace proc {

class FriendWriter {
  public:
    struct FriendConfig {
        FriendConfig()
            : output_dir(std::filesystem::path{"friends"}),
              compression_algo(ROOT::kZSTD),
              compression_level(4),
              tree_name("meta") {}

        std::filesystem::path output_dir;
        ROOT::ECompressionAlgorithm compression_algo;
        int compression_level;
        std::string tree_name;
    };

    explicit FriendWriter(const FriendConfig &config = FriendConfig{});

    std::filesystem::path writeFriend(ROOT::RDF::RNode df,
                                      const std::string &sample_key,
                                      const std::string &variation,
                                      const std::vector<std::string> &columns) const;

    std::filesystem::path writeFriendToPath(ROOT::RDF::RNode df,
                                            const std::filesystem::path &path,
                                            const std::vector<std::string> &columns) const;

  private:
    FriendConfig config_;

    std::filesystem::path generateFriendPath(const std::string &sample_key,
                                             const std::string &variation) const;
};

} // namespace proc

#endif
