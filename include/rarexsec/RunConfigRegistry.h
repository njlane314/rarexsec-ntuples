#ifndef RUN_CONFIG_REGISTRY_H
#define RUN_CONFIG_REGISTRY_H

#include <map>
#include <optional>
#include <stdexcept>
#include <string>

#include <rarexsec/RunConfig.h>

namespace proc {

class RunConfigRegistry {
  public:
    void addConfig(RunConfig config);

    const RunConfig &get(const std::string &beam, const std::string &period) const;

    const std::map<std::string, RunConfig> &all() const noexcept { return configs_; }

    void setBaseDirectory(std::string base_directory);
    const std::optional<std::string> &baseDirectory() const noexcept { return base_directory_; }

  private:
    std::map<std::string, RunConfig> configs_;
    std::optional<std::string> base_directory_;
};

}

#endif // RUN_CONFIG_REGISTRY_H
