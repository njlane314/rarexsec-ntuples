#include <rarexsec/WeightProcessor.h>

#include <cmath>

#include <rarexsec/LoggerUtils.h>

namespace proc {

WeightProcessor::WeightProcessor(const nlohmann::json &cfg, double total_run_pot, long total_run_triggers)
    : sample_pot_(cfg.value("pot", 0.0)),
      sample_triggers_(cfg.value("triggers", 0L)),
      total_run_pot_(total_run_pot),
      total_run_triggers_(total_run_triggers) {
    if (sample_pot_ <= 0.0 && sample_triggers_ <= 0L) {
        log::info("WeightProcessor::WeightProcessor", "[warning]", "sample has no scaling information");
    }
}

ROOT::RDF::RNode WeightProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    ROOT::RDF::RNode proc_df = df;
    if (st == SampleOrigin::kMonteCarlo || st == SampleOrigin::kDirt) {
        double scale = 1.0;
        if (sample_pot_ > 0.0 && total_run_pot_ > 0.0) {
            scale = total_run_pot_ / sample_pot_;
        }
        if (proc_df.HasColumn("base_event_weight")) {
            proc_df = proc_df.Redefine(
                "base_event_weight",
                [scale](double weight) { return weight * scale; },
                {"base_event_weight"});
        } else {
            proc_df = proc_df.Define("base_event_weight", [scale]() { return scale; });
        }

        const bool has_weight_spline = proc_df.HasColumn("weightSpline");
        const bool has_weight_tune = proc_df.HasColumn("weightTune");

        if (has_weight_spline && has_weight_tune) {
            proc_df = proc_df.Define(
                "nominal_event_weight",
                [](double w, float w_spline, float w_tune) {
                    double final_weight = w;
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
                {"base_event_weight",
                 "weightSpline",
                 "weightTune"});
        } else if (has_weight_spline) {
            proc_df = proc_df.Define(
                "nominal_event_weight",
                [](double w, float w_spline) {
                    double final_weight = w;
                    if (std::isfinite(w_spline) && w_spline > 0) {
                        final_weight *= w_spline;
                    }
                    if (!std::isfinite(final_weight) || final_weight < 0) {
                        return 1.0;
                    }
                    return final_weight;
                },
                {"base_event_weight",
                 "weightSpline"});
        } else if (has_weight_tune) {
            proc_df = proc_df.Define(
                "nominal_event_weight",
                [](double w, float w_tune) {
                    double final_weight = w;
                    if (std::isfinite(w_tune) && w_tune > 0) {
                        final_weight *= w_tune;
                    }
                    if (!std::isfinite(final_weight) || final_weight < 0) {
                        return 1.0;
                    }
                    return final_weight;
                },
                {"base_event_weight",
                 "weightTune"});
        }
    } else if (st == SampleOrigin::kExternal) {
        double scale = 1.0;
        if (sample_triggers_ > 0 && total_run_triggers_ > 0) {
            scale = static_cast<double>(total_run_triggers_) / static_cast<double>(sample_triggers_);
        }
        if (proc_df.HasColumn("base_event_weight")) {
            proc_df = proc_df.Redefine(
                "base_event_weight",
                [scale](double weight) { return weight * scale; },
                {"base_event_weight"});
        } else {
            proc_df = proc_df.Define("base_event_weight", [scale]() { return scale; });
        }
    }

    if (!proc_df.HasColumn("nominal_event_weight")) {
        if (proc_df.HasColumn("base_event_weight")) {
            // Use Define instead of Alias so that both columns can be materialised
            // simultaneously in downstream snapshots without ROOT considering them
            // duplicated requests for the same source branch.
            proc_df = proc_df.Define(
                "nominal_event_weight",
                [](double weight) { return weight; },
                {"base_event_weight"});
        } else {
            proc_df = proc_df.Define("nominal_event_weight", []() { return 1.0; });
        }
    }

    return next_ ? next_->process(proc_df, st) : proc_df;
}

}
