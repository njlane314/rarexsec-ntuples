#ifndef WEIGHT_PROCESSOR_H
#define WEIGHT_PROCESSOR_H

#include <cmath>

#include <nlohmann/json.hpp>

#include <rarexsec/utils/Logger.h>
#include <rarexsec/data/IEventProcessor.h>
#include <rarexsec/data/SampleTypes.h>

namespace analysis {

class WeightProcessor : public IEventProcessor {
  public:
    WeightProcessor(const nlohmann::json &cfg, double total_run_pot,
                    long total_run_triggers)
        : sample_pot_(cfg.value("pot", 0.0)),
          sample_triggers_(cfg.value("triggers", 0L)),
          total_run_pot_(total_run_pot),
          total_run_triggers_(total_run_triggers) {
        if (sample_pot_ <= 0.0 && sample_triggers_ <= 0L) {
            log::warn("WeightProcessor::WeightProcessor",
                      "sample JSON has no or invalid 'pot' or 'triggers';",
                      "base_event_weight will default to 1");
        }
    }

    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override {
        if (st == SampleOrigin::kMonteCarlo || st == SampleOrigin::kDirt) {
            double scale = 1.0;
            if (sample_pot_ > 0.0 && total_run_pot_ > 0.0) {
                scale = total_run_pot_ / sample_pot_;
            }
            df = df.Define("base_event_weight", [scale]() { return scale; });

            df = df.Define(
                "nominal_event_weight",
                [](double w, float w_spline, float w_tune) {
                    double final_weight = w;
                    if (std::isfinite(w_spline) && w_spline > 0)
                        final_weight *= w_spline;
                    if (std::isfinite(w_tune) && w_tune > 0)
                        final_weight *= w_tune;
                    if (!std::isfinite(final_weight) || final_weight < 0)
                        return 1.0;
                    return final_weight;
                },
                {"base_event_weight", "weightSpline", "weightTune"});
        } else if (st == SampleOrigin::kExternal) {
            double scale = 1.0;
            if (sample_triggers_ > 0 && total_run_triggers_ > 0) {
                scale = static_cast<double>(total_run_triggers_) /
                        static_cast<double>(sample_triggers_);
            }
            df = df.Define("base_event_weight", [scale]() { return scale; });
        }

        if (!df.HasColumn("nominal_event_weight")) {
            if (df.HasColumn("base_event_weight")) {
                df = df.Alias("nominal_event_weight", "base_event_weight");
            } else {
                df = df.Define("nominal_event_weight", []() { return 1.0; });
            }
        }

        if (next_) {
            return next_->process(df, st);
        }
        return df;
    }

  private:
    double sample_pot_;
    long sample_triggers_;
    double total_run_pot_;
    long total_run_triggers_;
};

}  // namespace analysis

#endif

