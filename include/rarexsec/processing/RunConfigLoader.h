#ifndef CONFIG_LOADER_H
#define CONFIG_LOADER_H

#include <fstream>
#include <string>

#include "nlohmann/json.hpp"

#include <rarexsec/utils/Logger.h>
#include <rarexsec/data/RunConfig.h>
#include <rarexsec/data/RunConfigRegistry.h>

namespace analysis {

class RunConfigLoader {
  public:
    static inline void loadFromJson(const nlohmann::json &data, RunConfigRegistry &registry) {
        const std::string top_key = data.contains("run_configurations") ? "run_configurations" : "beamlines";
        for (auto const &[beam, run_configs] : data.at(top_key).items()) {
            for (auto const &[run_period, run_details] : run_configs.items()) {
                RunConfig config(run_details, beam, run_period);
                config.validate();
                registry.addConfig(std::move(config));
            }
        }
    }
    static inline void loadFromFile(const std::string &config_path, RunConfigRegistry &registry) {
        std::ifstream f(config_path);
        if (!f.is_open()) {
            log::fatal("RunConfigLoader::loadFromFile", "Could not open config file:", config_path);
        }
        try {
            nlohmann::json data = nlohmann::json::parse(f);
            loadFromJson(data, registry);
        } catch (const std::exception &e) {
            log::fatal("RunConfigLoader::loadFromFile", "Parsing error:", e.what());
        }
    }
};

}

#endif
