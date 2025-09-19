#include <rarexsec/processing/NuMuCCSelectionProcessor.h>

namespace analysis {

ROOT::RDF::RNode NuMuCCSelectionProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    (void)st;
    auto selection_df = df.Define(
        "passes_numu_cc",
        [](bool pass_final, bool quality) { return pass_final && quality; },
        {"pass_final", "quality_event"})
                             .Define("selection_weight",
                                     [](bool selected, double weight) { return selected ? weight : 0.0; },
                                     {"passes_numu_cc", "nominal_event_weight"});
    return next_ ? next_->process(selection_df, st) : selection_df;
}

}
