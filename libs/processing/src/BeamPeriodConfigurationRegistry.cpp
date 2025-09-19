#include <rarexsec/processing/BeamPeriodConfigurationRegistry.h>

namespace proc {

void BeamPeriodConfigurationRegistry::addConfig(BeamPeriodConfiguration config) {
    auto key = config.label();
    if (configs_.count(key)) {
        throw std::runtime_error("Duplicate BeamPeriodConfiguration label: " + key);
    }
    configs_.emplace(std::move(key), std::move(config));
}

const BeamPeriodConfiguration &BeamPeriodConfigurationRegistry::get(const std::string &beam,
                                                                    const std::string &period) const {
    auto key = beam + ":" + period;
    auto it = configs_.find(key);
    if (it == configs_.end()) {
        throw std::out_of_range("BeamPeriodConfiguration not found: " + key);
    }
    return it->second;
}

void BeamPeriodConfigurationRegistry::setBaseDirectory(std::string base_directory) {
    base_directory_ = std::move(base_directory);
}

void BeamPeriodConfigurationRegistry::setRecipeHash(std::string recipe_hash) {
    recipe_hash_ = std::move(recipe_hash);
}

}
