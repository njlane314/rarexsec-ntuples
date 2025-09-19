#include <rarexsec/processing/RunConfigLoader.h>

#include <fstream>

#include <rarexsec/utils/Logger.h>

namespace analysis {

void RunConfigLoader::loadFromJson(const nlohmann::json &data, RunConfigRegistry &registry) {
    const nlohmann::json *scope = &data;
    if (auto samples_it = data.find("samples"); samples_it != data.end()) {
        if (!samples_it->is_object()) {
            log::fatal("RunConfigLoader::loadFromJson", "\"samples\" entry must be an object");
        }
        scope = &(*samples_it);
    }

    const auto beamlines_it = scope->find("beamlines");
    if (beamlines_it == scope->end()) {
        log::fatal("RunConfigLoader::loadFromJson",
                   "Missing beamlines section in configuration payload");
    }

    if (!beamlines_it->is_object()) {
        log::fatal("RunConfigLoader::loadFromJson", "\"beamlines\" section must be an object");
    }

    for (auto const &[beam, run_configs] : beamlines_it->items()) {
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
