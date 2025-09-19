#include <rarexsec/WeightProcessor.h>

#include <cmath>

#include <rarexsec/Logger.h>

namespace proc {

WeightProcessor::WeightProcessor(const nlohmann::json &cfg, double total_run_pot, long total_run_triggers)
    : sample_pot_(cfg.value("pot", 0.0)),
      sample_triggers_(cfg.value("triggers", 0L)),
      total_run_pot_(total_run_pot),
      total_run_triggers_(total_run_triggers) {
    if (sample_pot_ <= 0.0 && sample_triggers_ <= 0L) {
        log::warn("WeightProcessor::WeightProcessor", "sample has no scaling information");
    }
}

ROOT::RDF::RNode WeightProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    ROOT::RDF::RNode proc_df = df;
    if (st == SampleOrigin::kMonteCarlo || st == SampleOrigin::kDirt) {
        double scale = 1.0;
        if (sample_pot_ > 0.0 && total_run_pot_ > 0.0) {
            scale = total_run_pot_ / sample_pot_;
        }
        proc_df = proc_df.Define(
            "event_weight",
            [scale](float w_spline, float w_tune) {
                double final_weight = scale;
                if (std::isfinite(w_spline) && w_spline > 0) {
                    final_weight *= w_spline;
                }
                if (std::isfinite(w_tune) && w_tune > 0) {
                    final_weight *= w_tune;
                }
                if (!std::isfinite(final_weight) || final_weight < 0) {
                    return 1.0;
                }
                return final_weight;
            },
            {"weightSpline", "weightTune"});
    } else if (st == SampleOrigin::kExternal) {
        double scale = 1.0;
        if (sample_triggers_ > 0 && total_run_triggers_ > 0) {
            scale = static_cast<double>(total_run_triggers_) / static_cast<double>(sample_triggers_);
        }
        proc_df = proc_df.Define("event_weight", [scale]() { return scale; });
    }

    if (!proc_df.HasColumn("event_weight")) {
        proc_df = proc_df.Define("event_weight", []() { return 1.0; });
    }

    return next_ ? next_->process(proc_df, st) : proc_df;
}

}
