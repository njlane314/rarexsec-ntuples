#ifndef RAREXSEC_BEAM_PERIOD_CONFIG_LOADER_H
#define RAREXSEC_BEAM_PERIOD_CONFIG_LOADER_H

#include <string>

#include <nlohmann/json.hpp>

#include <rarexsec/BeamPeriodConfigRegistry.h>

namespace proc {

class BeamPeriodConfigLoader {
  public:
    static void loadFromJson(const nlohmann::json &data, BeamPeriodConfigRegistry &registry);
    static void loadFromFile(const std::string &config_path, BeamPeriodConfigRegistry &registry);
};

}

#endif
