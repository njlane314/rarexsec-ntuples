#include <rarexsec/PreselectionProcessor.h>
#include <rarexsec/SelectionCatalogue.h>

namespace proc {

ROOT::RDF::RNode PreselectionProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto base_df = ensureGenerationCount(df, "n_pfps_gen2", 2u);
    auto trigger_df = ensureSoftwareTrigger(base_df, st);

    auto pre_df = trigger_df.Define(
        "pass_pre",
        [st](float pe_beam, float pe_veto, bool swtrig) {
            return passesDatasetGate(st, pe_beam, pe_veto) && swtrig;
        },
        {"optical_filter_pe_beam", "optical_filter_pe_veto", "software_trigger"});

    auto flash_df = pre_df.Define("pass_flash", isSingleGoodSlice,
                                  {"num_slices", "topological_score"});

    auto fv_df = flash_df.Define("pass_fv", isInFiducialVolumeWithGap,
                                 {"reco_neutrino_vertex_sce_x", "reco_neutrino_vertex_sce_y",
                                  "reco_neutrino_vertex_sce_z"});

    auto final_df = fv_df.Define("pass_mu", "n_muons_tot > 0")
                         .Define("pass_topo", passesSliceQuality,
                                 {"contained_fraction", "slice_cluster_fraction"})
                         .Define(
                             "pass_final",
                             [](bool pre, bool flash, bool fv, bool mu, bool topo) {
                                 return pre && flash && fv && mu && topo;
                             },
                             {"pass_pre", "pass_flash", "pass_fv", "pass_mu", "pass_topo"});

    return next_ ? next_->process(final_df, st) : final_df;
}

}
