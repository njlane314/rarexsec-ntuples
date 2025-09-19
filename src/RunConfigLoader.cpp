#include <rarexsec/processing/RunConfigLoader.h>

#include <fstream>

#include <rarexsec/utils/Logger.h>

namespace analysis {

void RunConfigLoader::loadFromJson(const nlohmann::json &data, RunConfigRegistry &registry) {
    const std::string top_key = data.contains("run_configurations") ? "run_configurations" : "beamlines";
    for (auto const &[beam, run_configs] : data.at(top_key).items()) {
        for (auto const &[run_period, run_details] : run_configs.items()) {
            RunConfig config(run_details, beam, run_period);
            config.validate();
            registry.addConfig(std::move(config));
        }
    }
}

void RunConfigLoader::loadFromFile(const std::string &config_path, RunConfigRegistry &registry) {
    std::ifstream f(config_path);
    if (!f.is_open()) {
        log::fatal("RunConfigLoader::loadFromFile", "Could not open config file", config_path);
    }
    try {
        nlohmann::json data = nlohmann::json::parse(f);
        loadFromJson(data, registry);
    } catch (const std::exception &e) {
        log::fatal("RunConfigLoader::loadFromFile", "Parsing error", e.what());
    }
}

}
