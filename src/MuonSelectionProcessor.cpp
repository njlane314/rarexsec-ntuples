#include <rarexsec/MuonSelectionProcessor.h>
#include <rarexsec/Catalogue.h>

#include <cmath>

namespace proc {

ROOT::RDF::RNode MuonSelectionProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto proc_df = df;
    if (!df.HasColumn("track_shower_scores")) {
        proc_df = df.Define("n_muons_tot", []() { return 0UL; }).Define("has_muon", []() { return false; });
        return next_ ? next_->process(proc_df, st) : proc_df;
    }

    auto muon_mask_df = buildMuonMask(df);
    auto muon_features_df = extractMuonFeatures(muon_mask_df);
    return next_ ? next_->process(muon_features_df, st) : muon_features_df;
}

ROOT::RDF::RNode MuonSelectionProcessor::buildMuonMask(ROOT::RDF::RNode df) const {
    return df.Define(
        "muon_mask",
        [](const ROOT::RVec<float> &scores, const ROOT::RVec<float> &llr, const ROOT::RVec<float> &lengths,
           const ROOT::RVec<float> &dists, const ROOT::RVec<float> &start_x, const ROOT::RVec<float> &start_y,
           const ROOT::RVec<float> &start_z, const ROOT::RVec<float> &end_x, const ROOT::RVec<float> &end_y,
           const ROOT::RVec<float> &end_z, const ROOT::RVec<unsigned> &gens, const auto &plane_hits_u,
           const auto &plane_hits_v, const auto &plane_hits_y) {
            ROOT::RVec<bool> mask(scores.size(), false);
            for (std::size_t i = 0; i < scores.size(); ++i) {
                mask[i] = selection::isMuonCandidate(scores[i], llr[i], lengths[i], dists[i], gens[i], start_x[i],
                                                     start_y[i], start_z[i], end_x[i], end_y[i], end_z[i],
                                                     plane_hits_u[i], plane_hits_v[i], plane_hits_y[i]);
            }
            return mask;
        },
        {"track_shower_scores", "trk_llr_pid_v", "track_length", "track_distance_to_vertex", "track_start_x",
         "track_start_y", "track_start_z", "track_end_x", "track_end_y", "track_end_z", "pfp_generations",
         "pfp_num_plane_hits_U", "pfp_num_plane_hits_V", "pfp_num_plane_hits_Y"});
}

ROOT::RDF::RNode MuonSelectionProcessor::extractMuonFeatures(ROOT::RDF::RNode df) const {
    auto mu_df = df.Define("muon_trk_score_v", selection::filterByMask<float>, {"track_shower_scores", "muon_mask"})
                     .Define("muon_trk_llr_pid_v", selection::filterByMask<float>, {"trk_llr_pid_v", "muon_mask"})
                     .Define("muon_trk_start_x_v", selection::filterByMask<float>, {"track_start_x", "muon_mask"})
                     .Define("muon_trk_start_y_v", selection::filterByMask<float>, {"track_start_y", "muon_mask"})
                     .Define("muon_trk_start_z_v", selection::filterByMask<float>, {"track_start_z", "muon_mask"})
                     .Define("muon_trk_end_x_v", selection::filterByMask<float>, {"track_end_x", "muon_mask"})
                     .Define("muon_trk_end_y_v", selection::filterByMask<float>, {"track_end_y", "muon_mask"})
                     .Define("muon_trk_end_z_v", selection::filterByMask<float>, {"track_end_z", "muon_mask"})
                     .Define("muon_trk_length_v", selection::filterByMask<float>, {"track_length", "muon_mask"})
                     .Define("muon_trk_distance_v", selection::filterByMask<float>,
                             {"track_distance_to_vertex", "muon_mask"})
                     .Define("muon_pfp_generation_v", selection::filterByMask<unsigned>,
                             {"pfp_generations", "muon_mask"})
                     .Define(
                         "muon_track_costheta",
                             [](const ROOT::RVec<float> &theta, const ROOT::RVec<bool> &mask) {
                             return selection::transformByMask(theta, mask, [](float angle) { return std::cos(angle); });
                         },
                         {"track_theta", "muon_mask"})
                     .Define("n_muons_tot", "ROOT::VecOps::Sum(muon_mask)")
                     .Define("has_muon", "n_muons_tot > 0");

    return mu_df;
}

}
