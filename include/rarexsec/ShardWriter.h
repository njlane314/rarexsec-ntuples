#ifndef SHARD_WRITER_H
#define SHARD_WRITER_H

#include <filesystem>
#include <string>
#include <vector>

#include "ROOT/RDataFrame.hxx"

#include <rarexsec/HubCatalog.h>
#include <rarexsec/SampleTypes.h>

namespace proc {

class ShardWriter {
  public:
    struct ShardConfig {
        ShardConfig()
            : output_dir(std::filesystem::path{"shards"}),
              compression_algo(ROOT::kZSTD),
              compression_level(3) {}

        std::filesystem::path output_dir;
        int compression_algo;
        int compression_level;
    };

    explicit ShardWriter(const ShardConfig &config = ShardConfig{});

    std::vector<ShardEntry> writeShards(
        ROOT::RDF::RNode &df,
        const std::string &sample_key,
        const std::string &beam,
        const std::string &period,
        const std::string &variation,
        const std::string &stage,
        SampleOrigin origin,
        double dataset_pot,
        long dataset_triggers,
        const std::vector<std::string> &columns
    );

  private:
    ShardConfig config_;

    std::filesystem::path generateShardPath(
        const std::string &sample_key,
        const std::string &variation,
        uint32_t shard_index
    ) const;
};

} // namespace proc

#endif
