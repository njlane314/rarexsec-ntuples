#include <rarexsec/RunConfig.h>

#include <set>
#include <utility>

#include <rarexsec/LoggerUtils.h>

namespace proc {

RunConfig::RunConfig(const json &j, std::string beam_mode, std::string run_period)
    : beam_mode_(std::move(beam_mode)),
      run_period_(std::move(run_period)),
      nominal_pot_(j.value("nominal_pot",
                           j.value("pot_target_wcut_total", j.value("torb_target_pot_wcut", 0.0)))),
      nominal_triggers_(j.value("nominal_triggers",
                                j.value("ext_triggers_total", j.value("ext_triggers", 0L)))),
      samples_(j.at("samples")) {}

const std::string &RunConfig::beamMode() const noexcept { return beam_mode_; }
const std::string &RunConfig::runPeriod() const noexcept { return run_period_; }

double RunConfig::nominalPot() const noexcept { return nominal_pot_; }
long RunConfig::nominalTriggers() const noexcept { return nominal_triggers_; }

const RunConfig::json &RunConfig::sampleConfigs() const noexcept { return samples_; }

std::string RunConfig::label() const { return beam_mode_ + ":" + run_period_; }

void RunConfig::validate() const {
    if (beam_mode_.empty()) {
        log::fatal("RunConfig::validate", "empty beam_mode");
    }
    if (run_period_.empty()) {
        log::fatal("RunConfig::validate", "empty run_period");
    }
    if (samples_.empty()) {
        log::fatal("RunConfig::validate", "no samples for", beam_mode_ + "/" + run_period_);
    }

    std::set<std::string> keys;
    for (auto &sample : samples_) {
        std::string key = sample.at("sample_key").get<std::string>();
        if (!keys.insert(key).second) {
            log::fatal("RunConfig::validate", "duplicate sample key", key);
        }
    }
}

}
