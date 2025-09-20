#include <rarexsec/ReconstructionProcessor.h>
#include <rarexsec/Catalogue.h>

namespace proc {

ROOT::RDF::RNode ReconstructionProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto fiducial_df = df.Define(
        "in_reco_fiducial",
        [](float x, float y, float z) { return isInFiducialVolumeWithGap(x, y, z); },
        {"reco_neutrino_vertex_sce_x", "reco_neutrino_vertex_sce_y", "reco_neutrino_vertex_sce_z"});

    auto gen2_df = ensureGenerationCount(fiducial_df, "n_pfps_gen2", 2u);
    auto gen3_df = ensureGenerationCount(gen2_df, "n_pfps_gen3", 3u);
    auto trigger_df = ensureSoftwareTrigger(gen3_df, st);

    auto quality_df = trigger_df.Define(
        "quality_event",
        [st](float pe_beam, float pe_veto, int num_slices, float topo, float x, float y, float z, float contained_frac,
             float associated_frac) {
            const bool dataset_gate = passesDatasetGate(st, pe_beam, pe_veto, true);
            const bool basic_reco = isSingleGoodSlice(num_slices, topo);
            const bool fv = isInFiducialVolumeWithGap(x, y, z);
            const bool slice_quality = passesSliceQuality(contained_frac, associated_frac);
            return dataset_gate && basic_reco && fv && slice_quality;
        },
        {"optical_filter_pe_beam", "optical_filter_pe_veto", "num_slices", "topological_score",
         "reco_neutrino_vertex_sce_x", "reco_neutrino_vertex_sce_y", "reco_neutrino_vertex_sce_z",
         "contained_fraction", "slice_cluster_fraction"});

    return next_ ? next_->process(quality_df, st) : quality_df;
}

}
