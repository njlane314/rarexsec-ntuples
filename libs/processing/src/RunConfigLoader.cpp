#include <rarexsec/processing/RunConfigLoader.h>

#include <fstream>
#include <iterator>
#include <stdexcept>

#include <rarexsec/utils/Logger.h>

namespace proc {

void RunConfigLoader::loadFromJson(const nlohmann::json &data, RunConfigRegistry &registry) {
    if (!registry.catalogJson()) {
        registry.setCatalogJson(data.dump(4));
    }

    if (data.contains("source_recipe_hash") && data.at("source_recipe_hash").is_string()) {
        registry.setCatalogHash(data.at("source_recipe_hash").get<std::string>());
    } else if (data.contains("catalog_hash") && data.at("catalog_hash").is_string()) {
        registry.setCatalogHash(data.at("catalog_hash").get<std::string>());
    }

    if (data.contains("ntuple_base_directory") && data.at("ntuple_base_directory").is_string()) {
        registry.setBaseDirectory(data.at("ntuple_base_directory").get<std::string>());
    } else if (data.contains("samples")) {
        const auto &samples = data.at("samples");
        if (samples.contains("ntupledir") && samples.at("ntupledir").is_string()) {
            registry.setBaseDirectory(samples.at("ntupledir").get<std::string>());
        }
    }

    const nlohmann::json *run_configs_root = nullptr;
    if (data.contains("run_configurations")) {
        run_configs_root = &data.at("run_configurations");
    } else if (data.contains("beamlines")) {
        run_configs_root = &data.at("beamlines");
    } else if (data.contains("samples") && data.at("samples").contains("beamlines")) {
        run_configs_root = &data.at("samples").at("beamlines");
    }

    if (run_configs_root == nullptr) {
        throw std::runtime_error("RunConfigLoader::loadFromJson: missing run configuration sections");
    }

    for (auto const &[beam, run_configs] : run_configs_root->items()) {
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
    std::string config_text((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    registry.setCatalogJson(config_text);
    try {
        nlohmann::json data = nlohmann::json::parse(config_text);
        loadFromJson(data, registry);
    } catch (const std::exception &e) {
        log::fatal("RunConfigLoader::loadFromFile", "Parsing error", e.what());
    }
}

}
