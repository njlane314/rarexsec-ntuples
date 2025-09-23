#include <rarexsec/ReconstructionProcessor.h>
#include <rarexsec/SelectionCatalogue.h>

namespace proc {

ROOT::RDF::RNode ReconstructionProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto fiducial_df = df.Define(
        "in_reco_fiducial",
        [](float x, float y, float z) { return selc::isInFiducialVolumeWithGap(x, y, z); },
        {"reco_neutrino_vertex_sce_x",
         "reco_neutrino_vertex_sce_y",
         "reco_neutrino_vertex_sce_z"});

    auto gen2_df = selc::ensureGenerationCount(fiducial_df, "n_pfps_gen2", 2u);
    auto gen3_df = selc::ensureGenerationCount(gen2_df, "n_pfps_gen3", 3u);
    auto trigger_df = selc::ensureSoftwareTrigger(gen3_df, st);

    auto quality_df = trigger_df.Define(
        "quality_event",
        [st](float pe_beam, float pe_veto, bool software_trigger, int num_slices, float topo, float x, float y, float z,
             float contained_frac, float associated_frac) {
            const bool gate_and_trigger =
                selc::passesDatasetGateAndTrigger(st, pe_beam, pe_veto, software_trigger, true);
            const bool quality_cuts =
                selc::passesQualityCuts(num_slices, topo, x, y, z, contained_frac, associated_frac);
            return gate_and_trigger && quality_cuts;
        },
        {"optical_filter_pe_beam",
         "optical_filter_pe_veto",
         "software_trigger",
         "num_slices",
         "topological_score",
         "reco_neutrino_vertex_sce_x",
         "reco_neutrino_vertex_sce_y",
         "reco_neutrino_vertex_sce_z",
         "contained_fraction",
         "slice_cluster_fraction"});

    return quality_df;
}

}
