#ifndef RAREXSEC_BEAM_PERIOD_CONFIG_H
#define RAREXSEC_BEAM_PERIOD_CONFIG_H

#include <string>

#include <nlohmann/json.hpp>

namespace proc {

class BeamPeriodConfig {
  public:
    using json = nlohmann::json;

    BeamPeriodConfig(const json &j, std::string beam_mode, std::string run_period);

    const std::string &beamMode() const noexcept;
    const std::string &runPeriod() const noexcept;
    double nominalPot() const noexcept;
    long nominalTriggers() const noexcept;
    const json &sampleConfigs() const noexcept;

    std::string label() const;
    void validate() const;

  private:
    std::string beam_mode_;
    std::string run_period_;
    double nominal_pot_;
    long nominal_triggers_;
    json samples_;
};

}

#endif
