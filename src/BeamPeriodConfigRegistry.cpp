#include <rarexsec/BeamPeriodConfigRegistry.h>

namespace proc {

void BeamPeriodConfigRegistry::addConfig(BeamPeriodConfig config) {
    auto key = config.label();
    if (configs_.count(key)) {
        throw std::runtime_error("Duplicate BeamPeriodConfig label: " + key);
    }
    configs_.emplace(std::move(key), std::move(config));
}

const BeamPeriodConfig &BeamPeriodConfigRegistry::get(const std::string &beam,
                                                      const std::string &period) const {
    auto key = beam + ":" + period;
    auto it = configs_.find(key);
    if (it == configs_.end()) {
        throw std::out_of_range("BeamPeriodConfig not found: " + key);
    }
    return it->second;
}

void BeamPeriodConfigRegistry::setBaseDirectory(std::string base_directory) {
    base_directory_ = std::move(base_directory);
}

}
