#include <rarexsec/BeamPeriodConfigurationLoader.h>

#include <fstream>
#include <stdexcept>

#include <rarexsec/Logger.h>

namespace proc {

void BeamPeriodConfigurationLoader::loadFromJson(const nlohmann::json &data,
                                                 BeamPeriodConfigurationRegistry &registry) {
    if (data.contains("ntuple_base_directory") && data.at("ntuple_base_directory").is_string()) {
        registry.setBaseDirectory(data.at("ntuple_base_directory").get<std::string>());
    } else if (data.contains("samples")) {
        const auto &samples = data.at("samples");
        if (samples.contains("ntupledir") && samples.at("ntupledir").is_string()) {
            registry.setBaseDirectory(samples.at("ntupledir").get<std::string>());
        }
    }

    const auto assign_recipe_hash = [&registry](const nlohmann::json &section) {
        if (section.contains("source_recipe_hash") && section.at("source_recipe_hash").is_string()) {
            registry.setRecipeHash(section.at("source_recipe_hash").get<std::string>());
            return true;
        }
        if (section.contains("recipe_hash") && section.at("recipe_hash").is_string()) {
            registry.setRecipeHash(section.at("recipe_hash").get<std::string>());
            return true;
        }
        return false;
    };

    if (!assign_recipe_hash(data) && data.contains("samples") && data.at("samples").is_object()) {
        assign_recipe_hash(data.at("samples"));
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
        throw std::runtime_error(
            "BeamPeriodConfigurationLoader::loadFromJson: missing run configuration sections");
    }

    for (auto const &[beam, run_configs] : run_configs_root->items()) {
        for (auto const &[run_period, run_details] : run_configs.items()) {
            BeamPeriodConfiguration config(run_details, beam, run_period);
            config.validate();
            registry.addConfig(std::move(config));
        }
    }
}

void BeamPeriodConfigurationLoader::loadFromFile(const std::string &config_path,
                                                 BeamPeriodConfigurationRegistry &registry) {
    std::ifstream f(config_path);
    if (!f.is_open()) {
        log::fatal("BeamPeriodConfigurationLoader::loadFromFile", "Could not open config file", config_path);
    }
    try {
        nlohmann::json data = nlohmann::json::parse(f);
        loadFromJson(data, registry);
    } catch (const std::exception &e) {
        log::fatal("BeamPeriodConfigurationLoader::loadFromFile", "Parsing error", e.what());
    }
}

}
