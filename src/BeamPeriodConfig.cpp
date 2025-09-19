#include <rarexsec/BeamPeriodConfig.h>

#include <set>
#include <utility>

#include <rarexsec/Logger.h>

namespace proc {

BeamPeriodConfig::BeamPeriodConfig(const json &j, std::string beam_mode, std::string run_period)
    : beam_mode_(std::move(beam_mode)),
      run_period_(std::move(run_period)),
      nominal_pot_(j.value("nominal_pot",
                           j.value("pot_target_wcut_total", j.value("torb_target_pot_wcut", 0.0)))),
      nominal_triggers_(j.value("nominal_triggers",
                                j.value("ext_triggers_total", j.value("ext_triggers", 0L)))),
      samples_(j.at("samples")) {}

const std::string &BeamPeriodConfig::beamMode() const noexcept { return beam_mode_; }

const std::string &BeamPeriodConfig::runPeriod() const noexcept { return run_period_; }

double BeamPeriodConfig::nominalPot() const noexcept { return nominal_pot_; }

long BeamPeriodConfig::nominalTriggers() const noexcept { return nominal_triggers_; }

const BeamPeriodConfig::json &BeamPeriodConfig::sampleConfigs() const noexcept { return samples_; }

std::string BeamPeriodConfig::label() const { return beam_mode_ + ":" + run_period_; }

void BeamPeriodConfig::validate() const {
    if (beam_mode_.empty()) {
        log::fatal("BeamPeriodConfig::validate", "empty beam_mode");
    }
    if (run_period_.empty()) {
        log::fatal("BeamPeriodConfig::validate", "empty run_period");
    }
    if (samples_.empty()) {
        log::fatal("BeamPeriodConfig::validate", "no samples for", beam_mode_ + "/" + run_period_);
    }

    std::set<std::string> keys;
    for (auto &sample : samples_) {
        std::string key = sample.at("sample_key").get<std::string>();
        if (!keys.insert(key).second) {
            log::fatal("BeamPeriodConfig::validate", "duplicate sample key", key);
        }
    }
}

}
