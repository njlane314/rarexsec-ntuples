#ifndef RAREXSEC_PROCESSING_BEAM_PERIOD_CONFIGURATION_LOADER_H
#define RAREXSEC_PROCESSING_BEAM_PERIOD_CONFIGURATION_LOADER_H

#include <string>

#include <nlohmann/json.hpp>

#include <rarexsec/processing/BeamPeriodConfigurationRegistry.h>

namespace proc {

class BeamPeriodConfigurationLoader {
  public:
    static void loadFromJson(const nlohmann::json &data, BeamPeriodConfigurationRegistry &registry);
    static void loadFromFile(const std::string &config_path, BeamPeriodConfigurationRegistry &registry);
};

}

#endif
