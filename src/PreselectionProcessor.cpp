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

    auto final_df = fv_df.Define("pass_mu", "n_muons_tot > 0")
                         .Define("pass_topo", selc::passesSliceQuality,
                                 {"contained_fraction",
                                  "slice_cluster_fraction"})
                         .Define(
                             "pass_final",
                             [](bool pre, int num_slices, float topological_score, float x, float y, float z,
                                float contained_fraction, float cluster_fraction, bool mu) {
                                 return selc::passesQualityCuts(pre,
                                                                num_slices,
                                                                topological_score,
                                                                x,
                                                                y,
                                                                z,
                                                                contained_fraction,
                                                                cluster_fraction) &&
                                        mu;
                             },
                             {"pass_pre",
                              "num_slices",
                              "topological_score",
                              "reco_neutrino_vertex_sce_x",
                              "reco_neutrino_vertex_sce_y",
                              "reco_neutrino_vertex_sce_z",
                              "contained_fraction",
                              "slice_cluster_fraction",
                              "pass_mu"});

    return final_df;
}

}
