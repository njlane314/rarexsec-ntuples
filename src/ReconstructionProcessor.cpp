#include <rarexsec/processing/ReconstructionProcessor.h>

namespace proc {

ROOT::RDF::RNode ReconstructionProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto base_df = df.Define(
        "in_reco_fiducial",
        "reco_neutrino_vertex_sce_x > 5 && reco_neutrino_vertex_sce_x < 251 && "
        "reco_neutrino_vertex_sce_y > -110 && reco_neutrino_vertex_sce_y < 110 && "
        "reco_neutrino_vertex_sce_z > 20 && reco_neutrino_vertex_sce_z < 986 && "
        "(reco_neutrino_vertex_sce_z < 675 || reco_neutrino_vertex_sce_z > 775)");

    auto gen2_df = base_df.Define(
        "n_pfps_gen2",
        [](const ROOT::RVec<unsigned> &gens) { return ROOT::VecOps::Sum(gens == 2u); },
        {"pfp_generations"});

    auto gen3_df = gen2_df.Define(
        "n_pfps_gen3",
        [](const ROOT::RVec<unsigned> &gens) { return ROOT::VecOps::Sum(gens == 3u); },
        {"pfp_generations"});

    ROOT::RDF::RNode swtrig_df = gen3_df;
    if (st == SampleOrigin::kMonteCarlo) {
        if (gen3_df.HasColumn("software_trigger_pre_ext")) {
            swtrig_df = gen3_df.Define("software_trigger",
                                       [](unsigned run, int pre, int post) { return run < 16880 ? pre > 0 : post > 0; },
                                       {"run", "software_trigger_pre_ext", "software_trigger_post_ext"});
        } else if (gen3_df.HasColumn("software_trigger_pre")) {
            swtrig_df = gen3_df.Define("software_trigger",
                                       [](unsigned run, int pre, int post) { return run < 16880 ? pre > 0 : post > 0; },
                                       {"run", "software_trigger_pre", "software_trigger_post"});
        } else if (gen3_df.HasColumn("software_trigger")) {
            swtrig_df = gen3_df.Redefine("software_trigger", "software_trigger != 0");
        } else {
            swtrig_df = gen3_df.Define("software_trigger", []() { return true; });
        }
    } else {
        if (gen3_df.HasColumn("software_trigger")) {
            swtrig_df = gen3_df.Redefine("software_trigger", "software_trigger != 0");
        } else {
            swtrig_df = gen3_df.Define("software_trigger", []() { return true; });
        }
    }

    auto quality_df = swtrig_df.Define(
        "quality_event",
        [st](float pe_beam, float pe_veto, int nslices, float topo, int n_gen2, float x, float y, float z,
             float contained_frac, float associated_frac) {
            bool dataset_gate = (st == SampleOrigin::kMonteCarlo) ? (pe_beam > 0.f && pe_veto < 20.f) : true;
            bool basic_reco = nslices == 1 && topo > 0.06f && n_gen2 > 1;
            bool fv = x > 5.f && x < 251.f && y > -110.f && y < 110.f && z > 20.f && z < 986.f &&
                      (z < 675.f || z > 775.f);
            bool slice_quality = contained_frac >= 0.7f && associated_frac >= 0.5f;
            return dataset_gate && basic_reco && fv && slice_quality;
        },
        {"optical_filter_pe_beam", "optical_filter_pe_veto", "num_slices", "topological_score", "n_pfps_gen2",
         "reco_neutrino_vertex_sce_x", "reco_neutrino_vertex_sce_y", "reco_neutrino_vertex_sce_z",
         "contained_fraction", "slice_cluster_fraction"});

    return next_ ? next_->process(quality_df, st) : quality_df;
}

}
