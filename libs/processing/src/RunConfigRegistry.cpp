#include <rarexsec/processing/RunConfigRegistry.h>

namespace proc {

void RunConfigRegistry::addConfig(RunConfig config) {
    auto key = config.label();
    if (configs_.count(key)) {
        throw std::runtime_error("Duplicate RunConfig label: " + key);
    }
    configs_.emplace(std::move(key), std::move(config));
}

const RunConfig &RunConfigRegistry::get(const std::string &beam, const std::string &period) const {
    auto key = beam + ":" + period;
    auto it = configs_.find(key);
    if (it == configs_.end()) {
        throw std::out_of_range("RunConfig not found: " + key);
    }
    return it->second;
}

void RunConfigRegistry::setBaseDirectory(std::string base_directory) {
    base_directory_ = std::move(base_directory);
}

void RunConfigRegistry::setCatalogJson(std::string catalog_json) {
    catalog_json_ = std::move(catalog_json);
}

void RunConfigRegistry::setCatalogHash(std::string catalog_hash) {
    catalog_hash_ = std::move(catalog_hash);
}

}
