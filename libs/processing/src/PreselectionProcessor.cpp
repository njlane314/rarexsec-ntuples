#include <rarexsec/processing/PreselectionProcessor.h>

namespace proc {

ROOT::RDF::RNode PreselectionProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto init_df = df;
    if (!df.HasColumn("n_pfps_gen2")) {
        init_df = df.Define(
            "n_pfps_gen2",
            [](const ROOT::RVec<unsigned> &gens) { return ROOT::VecOps::Sum(gens == 2u); },
            {"pfp_generations"});
    }

    auto proc_df = init_df;
    if (st == SampleOrigin::kMonteCarlo) {
        if (init_df.HasColumn("software_trigger_pre_ext")) {
            proc_df = init_df.Define(
                "software_trigger",
                [](unsigned run, int pre, int post) { return run < 16880 ? pre > 0 : post > 0; },
                {"run", "software_trigger_pre_ext", "software_trigger_post_ext"});
        } else if (init_df.HasColumn("software_trigger_pre")) {
            proc_df = init_df.Define(
                "software_trigger",
                [](unsigned run, int pre, int post) { return run < 16880 ? pre > 0 : post > 0; },
                {"run", "software_trigger_pre", "software_trigger_post"});
        } else if (init_df.HasColumn("software_trigger")) {
            proc_df = init_df.Redefine("software_trigger", "software_trigger != 0");
        } else {
            proc_df = init_df.Define("software_trigger", []() { return true; });
        }
    } else {
        if (init_df.HasColumn("software_trigger")) {
            proc_df = init_df.Redefine("software_trigger", "software_trigger != 0");
        } else {
            proc_df = init_df.Define("software_trigger", []() { return true; });
        }
    }

    proc_df = proc_df.Define("bnbdata", [st]() { return st == SampleOrigin::kData ? 1 : 0; })
                   .Define("extdata", [st]() { return st == SampleOrigin::kExternal ? 1 : 0; });

    auto pre_df = proc_df.Define(
        "pass_pre",
        [st](int bnb, int ext, float pe_beam, float pe_veto, bool swtrig) {
            bool dataset_gate = (bnb == 0 && ext == 0) ? (pe_beam > 0.f && pe_veto < 20.f) : true;
            if (st == SampleOrigin::kMonteCarlo) {
                dataset_gate = pe_beam > 0.f && pe_veto < 20.f;
            }
            return dataset_gate && swtrig;
        },
        {"bnbdata", "extdata", "optical_filter_pe_beam", "optical_filter_pe_veto", "software_trigger"});

    auto flash_df = pre_df.Define(
        "pass_flash",
        [](int num_slices, float topo, int n_gen2) { return num_slices == 1 && topo > 0.06f && n_gen2 > 1; },
        {"num_slices", "topological_score", "n_pfps_gen2"});

    auto fv_df = flash_df.Define(
        "pass_fv",
        [](float x, float y, float z) {
            return x > 5.f && x < 251.f && y > -110.f && y < 110.f && z > 20.f && z < 986.f &&
                   (z < 675.f || z > 775.f);
        },
        {"reco_neutrino_vertex_sce_x", "reco_neutrino_vertex_sce_y", "reco_neutrino_vertex_sce_z"});

    auto mu_df = fv_df.Define("pass_mu", "n_muons_tot > 0");

    auto topo_df = mu_df.Define(
        "pass_topo",
        [](float contained, float cluster) { return contained >= 0.7f && cluster >= 0.5f; },
        {"contained_fraction", "slice_cluster_fraction"});

    auto final_df = topo_df.Define(
        "pass_final",
        [](bool pre, bool flash, bool fv, bool mu, bool topo) { return pre && flash && fv && mu && topo; },
        {"pass_pre", "pass_flash", "pass_fv", "pass_mu", "pass_topo"});

    return next_ ? next_->process(final_df, st) : final_df;
}

}
