#ifndef RUN_CONFIG_H
#define RUN_CONFIG_H

#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <rarexsec/utils/Logger.h>
#include <rarexsec/data/SampleDefinition.h>

namespace analysis {

using json = nlohmann::json;

struct RunConfig {
    std::string beam_mode;
    std::string run_period;

    double nominal_pot{0.0};
    long nominal_triggers{0};

    json samples;

    RunConfig(const json &j, std::string bm, std::string pr)
        : beam_mode(std::move(bm)),
          run_period(std::move(pr)),
            nominal_pot(j.value("nominal_pot",
                                j.value("pot_target_wcut_total",
                                        j.value("torb_target_pot_wcut", 0.0)))),
          nominal_triggers(j.value("nominal_triggers",
                                   j.value("ext_triggers_total",
                                           j.value("ext_triggers", 0L)))),
          samples(j.at("samples")) {}

    std::string label() const { return beam_mode + ":" + run_period; }

    void validate() const {
        if (beam_mode.empty())
            log::fatal("RunConfig::validate", "empty beam_mode");
        if (run_period.empty())
            log::fatal("RunConfig::validate", "empty run_period");
        if (samples.empty())
            log::fatal("RunConfig::validate", "no samples for", beam_mode + "/" + run_period);

        std::set<std::string> keys;
        for (auto &s : samples) {
            std::string key = s.at("sample_key").get<std::string>();
            if (!keys.insert(key).second) {
                log::fatal("RunConfig::validate", "duplicate sample key:", key);
            }
        }
    }
};

}

#endif
