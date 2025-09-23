#include <rarexsec/WeightProcessor.h>

#include <cmath>

#include <rarexsec/LoggerUtils.h>

namespace {

constexpr const char *kBaseEventWeight = "base_event_weight";
constexpr const char *kNominalEventWeight = "nominal_event_weight";
constexpr const char *kSplineWeight = "weightSpline";
constexpr const char *kTuneWeight = "weightTune";

double computeExposureScale(double sample_pot, double total_run_pot) {
    if (sample_pot > 0.0 && total_run_pot > 0.0) {
        return total_run_pot / sample_pot;
    }
    return 1.0;
}

double computeTriggerScale(long sample_triggers, long total_run_triggers) {
    if (sample_triggers > 0 && total_run_triggers > 0) {
        return static_cast<double>(total_run_triggers) / static_cast<double>(sample_triggers);
    }
    return 1.0;
}

double rescaleBaseWeight(double weight, double scale) { return weight * scale; }

double createBaseWeightFromScale(double scale) { return scale; }

double passThroughWeight(double weight) { return weight; }

double defaultNominalWeight() { return 1.0; }

template <typename Factor>
void applyPositiveFiniteFactor(double &weight, Factor factor) {
    const double factor_value = static_cast<double>(factor);
    if (std::isfinite(factor_value) && factor_value > 0.0) {
        weight *= factor_value;
    }
}

template <typename... Factors>
double combineNominalWeight(double base_weight, Factors... factors) {
    double final_weight = base_weight;
    (applyPositiveFiniteFactor(final_weight, factors), ...);
    if (!std::isfinite(final_weight) || final_weight < 0.0) {
        return 1.0;
    }
    return final_weight;
}

double nominalWeightWithSplineAndTune(double base_weight, float w_spline, float w_tune) {
    return combineNominalWeight(base_weight, w_spline, w_tune);
}

double nominalWeightWithSpline(double base_weight, float w_spline) {
    return combineNominalWeight(base_weight, w_spline);
}

double nominalWeightWithTune(double base_weight, float w_tune) {
    return combineNominalWeight(base_weight, w_tune);
}

ROOT::RDF::RNode scaleBaseWeight(ROOT::RDF::RNode df, double scale) {
    if (df.HasColumn(kBaseEventWeight)) {
        return df.Redefine(kBaseEventWeight, [scale](double weight) { return rescaleBaseWeight(weight, scale); }, {kBaseEventWeight});
    }
    return df.Define(kBaseEventWeight, [scale]() { return createBaseWeightFromScale(scale); });
}

ROOT::RDF::RNode scaleBaseWeightByExposure(ROOT::RDF::RNode df, double exposure_scale) {
    return scaleBaseWeight(df, exposure_scale);
}

ROOT::RDF::RNode scaleBaseWeightByTriggers(ROOT::RDF::RNode df, double trigger_scale) {
    return scaleBaseWeight(df, trigger_scale);
}

ROOT::RDF::RNode defineNominalWeightWithSplineAndTune(ROOT::RDF::RNode df) {
    return df.Define(kNominalEventWeight, &nominalWeightWithSplineAndTune, {kBaseEventWeight, kSplineWeight, kTuneWeight});
}

ROOT::RDF::RNode defineNominalWeightWithSpline(ROOT::RDF::RNode df) {
    return df.Define(kNominalEventWeight, &nominalWeightWithSpline, {kBaseEventWeight, kSplineWeight});
}

ROOT::RDF::RNode defineNominalWeightWithTune(ROOT::RDF::RNode df) {
    return df.Define(kNominalEventWeight, &nominalWeightWithTune, {kBaseEventWeight, kTuneWeight});
}

ROOT::RDF::RNode defineNominalWeightFromBase(ROOT::RDF::RNode df) {
    if (df.HasColumn(kBaseEventWeight)) {
        return df.Define(kNominalEventWeight, &passThroughWeight, {kBaseEventWeight});
    }
    return df.Define(kNominalEventWeight, &defaultNominalWeight);
}

} // namespace

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
        const double exposure_scale = computeExposureScale(sample_pot_, total_run_pot_);
        proc_df = scaleBaseWeightByExposure(proc_df, exposure_scale);

        const bool has_weight_spline = proc_df.HasColumn(kSplineWeight);
        const bool has_weight_tune = proc_df.HasColumn(kTuneWeight);

        if (has_weight_spline && has_weight_tune) {
            proc_df = defineNominalWeightWithSplineAndTune(proc_df);
        } else if (has_weight_spline) {
            proc_df = defineNominalWeightWithSpline(proc_df);
        } else if (has_weight_tune) {
            proc_df = defineNominalWeightWithTune(proc_df);
        }
    } else if (st == SampleOrigin::kExternal) {
        const double trigger_scale = computeTriggerScale(sample_triggers_, total_run_triggers_);
        proc_df = scaleBaseWeightByTriggers(proc_df, trigger_scale);
    }

    if (!proc_df.HasColumn(kNominalEventWeight)) {
        proc_df = defineNominalWeightFromBase(proc_df);
    }

    return proc_df;
}

}
