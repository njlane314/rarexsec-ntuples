#ifndef RAREXSEC_PROCESSING_BEAM_PERIOD_CONFIGURATION_REGISTRY_H
#define RAREXSEC_PROCESSING_BEAM_PERIOD_CONFIGURATION_REGISTRY_H

#include <map>
#include <optional>
#include <stdexcept>
#include <string>

#include <rarexsec/processing/BeamPeriodConfiguration.h>

namespace proc {

class BeamPeriodConfigurationRegistry {
  public:
    void addConfig(BeamPeriodConfiguration config);

    const BeamPeriodConfiguration &get(const std::string &beam, const std::string &period) const;

    const std::map<std::string, BeamPeriodConfiguration> &all() const noexcept { return configs_; }

    void setBaseDirectory(std::string base_directory);
    const std::optional<std::string> &baseDirectory() const noexcept { return base_directory_; }

    void setRecipeHash(std::string recipe_hash);
    const std::optional<std::string> &recipeHash() const noexcept { return recipe_hash_; }

  private:
    std::map<std::string, BeamPeriodConfiguration> configs_;
    std::optional<std::string> base_directory_;
    std::optional<std::string> recipe_hash_;
};

}

#endif
