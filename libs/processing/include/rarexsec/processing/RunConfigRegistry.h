#ifndef RAREXSEC_PROCESSING_RUN_CONFIG_REGISTRY_H
#define RAREXSEC_PROCESSING_RUN_CONFIG_REGISTRY_H

#include <map>
#include <optional>
#include <stdexcept>
#include <string>

#include <rarexsec/processing/RunConfig.h>

namespace proc {

class RunConfigRegistry {
  public:
    void addConfig(RunConfig config);

    const RunConfig &get(const std::string &beam, const std::string &period) const;

    const std::map<std::string, RunConfig> &all() const noexcept { return configs_; }

    void setBaseDirectory(std::string base_directory);
    const std::optional<std::string> &baseDirectory() const noexcept { return base_directory_; }

    void setCatalogJson(std::string catalog_json);
    const std::optional<std::string> &catalogJson() const noexcept { return catalog_json_; }

    void setCatalogHash(std::string catalog_hash);
    const std::optional<std::string> &catalogHash() const noexcept { return catalog_hash_; }

  private:
    std::map<std::string, RunConfig> configs_;
    std::optional<std::string> base_directory_;
    std::optional<std::string> catalog_json_;
    std::optional<std::string> catalog_hash_;
};

}

#endif
