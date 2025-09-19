#ifndef RAREXSEC_PROCESSING_RUN_CONFIG_LOADER_H
#define RAREXSEC_PROCESSING_RUN_CONFIG_LOADER_H

#include <string>

#include <nlohmann/json.hpp>

#include <rarexsec/processing/RunConfigRegistry.h>

namespace proc {

class RunConfigLoader {
  public:
    static void loadFromJson(const nlohmann::json &data, RunConfigRegistry &registry);
    static void loadFromFile(const std::string &config_path, RunConfigRegistry &registry);
};

}

#endif
