#include <rarexsec/PreselectionProcessor.h>
#include <rarexsec/SelectionCatalogue.h>

namespace proc {

ROOT::RDF::RNode PreselectionProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto base_df = selc::ensureGenerationCount(df, "n_pfps_gen2", 2u);
    auto trigger_df = selc::ensureSoftwareTrigger(base_df, st);

    auto pre_df = trigger_df.Define(
        "pass_pre",
        [st](float pe_beam, float pe_veto, bool swtrig) {
            return selc::passesDatasetGateAndTrigger(st, pe_beam, pe_veto, swtrig);
        },
        {"optical_filter_pe_beam",
         "optical_filter_pe_veto",
         "software_trigger"});

    auto flash_df = pre_df.Define("pass_flash", selc::isSingleGoodSlice,
                                  {"num_slices",
                                   "topological_score"});

    auto fv_df = flash_df.Define("pass_fv", selc::isInFiducialVolumeWithGap,
                                 {"reco_neutrino_vertex_sce_x",
                                  "reco_neutrino_vertex_sce_y",
                                  "reco_neutrino_vertex_sce_z"});

    auto topo_df = fv_df.Define("pass_topo", selc::passesSliceQuality,
                                 {"contained_fraction",
                                  "slice_cluster_fraction"});

    auto quality_df = topo_df.Define(
        "pass_quality",
        [](bool pass_flash, bool pass_fv, bool pass_topo) {
            return selc::passesQualityCuts(pass_flash, pass_fv, pass_topo);
        },
        {"pass_flash",
         "pass_fv",
         "pass_topo"});

    auto final_df = quality_df.Define("pass_mu", "n_muons_tot > 0")
                              .Define(
                                  "pass_final",
                                  [](bool pre, bool quality, bool mu) {
                                      return pre && quality && mu;
                                  },
                                  {"pass_pre",
                                   "pass_quality",
                                   "pass_mu"});

    return final_df;
}

}
