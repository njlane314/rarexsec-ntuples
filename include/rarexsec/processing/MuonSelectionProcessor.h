#ifndef MUON_SELECTION_PROCESSOR_H
#define MUON_SELECTION_PROCESSOR_H

#include <cmath>

#include "ROOT/RVec.hxx"

#include <rarexsec/data/IEventProcessor.h>

namespace analysis {

class MuonSelectionProcessor : public IEventProcessor {
public:
  ROOT::RDF::RNode process(ROOT::RDF::RNode df,
                           SampleOrigin st) const override {
    if (!df.HasColumn("track_shower_scores")) {
      auto no_mu_df = df.Define("n_muons_tot", []() { return 0UL; })
                          .Define("has_muon", []() { return false; });
      return next_ ? next_->process(no_mu_df, st) : no_mu_df;
    }

    auto muon_mask_df = this->buildMuonMask(df);

    auto muon_features_df = this->extractMuonFeatures(muon_mask_df);

    return next_ ? next_->process(muon_features_df, st) : muon_features_df;
  }

private:
  ROOT::RDF::RNode buildMuonMask(ROOT::RDF::RNode df) const {
    return df.Define(
        "muon_mask",
        [](const ROOT::RVec<float> &scores, const ROOT::RVec<float> &llr,
           const ROOT::RVec<float> &lengths, const ROOT::RVec<float> &dists,
           const ROOT::RVec<float> &start_x, const ROOT::RVec<float> &start_y,
           const ROOT::RVec<float> &start_z, const ROOT::RVec<float> &end_x,
           const ROOT::RVec<float> &end_y, const ROOT::RVec<float> &end_z,
           const ROOT::RVec<unsigned> &gens,
           const ROOT::RVec<int> &hits_u, const ROOT::RVec<int> &hits_v,
           const ROOT::RVec<int> &hits_y) {
          ROOT::RVec<bool> mask(scores.size());
          const float min_x = 5.f, max_x = 251.f;
          const float min_y = -110.f, max_y = 110.f;
          const float min_z = 20.f, max_z = 986.f;
          for (size_t i = 0; i < scores.size(); ++i) {
            bool fid_start = start_x[i] > min_x && start_x[i] < max_x &&
                             start_y[i] > min_y && start_y[i] < max_y &&
                             start_z[i] > min_z && start_z[i] < max_z;
           bool fid_end = end_x[i] > min_x && end_x[i] < max_x &&
                          end_y[i] > min_y && end_y[i] < max_y &&
                          end_z[i] > min_z && end_z[i] < max_z;
            mask[i] =
                (scores[i] > 0.5f && llr[i] > 0.2f && lengths[i] > 10.0f &&
                 dists[i] < 4.0f && gens[i] == 2u && fid_start && fid_end);
          }
          return mask;
        },
        {"track_shower_scores", "trk_llr_pid_v", "track_length",
         "track_distance_to_vertex", "track_start_x", "track_start_y",
         "track_start_z", "track_end_x", "track_end_y", "track_end_z",
         "pfp_generations"});
  }

  ROOT::RDF::RNode extractMuonFeatures(ROOT::RDF::RNode df) const {
    auto filter_float = [](const ROOT::RVec<float> &vals,
                           const ROOT::RVec<bool> &mask) {
      ROOT::RVec<float> out;
      out.reserve(vals.size());
      for (size_t i = 0; i < vals.size(); ++i) {
        if (mask[i])
          out.push_back(vals[i]);
      }
      return out;
    };

    auto filter_uint = [](const ROOT::RVec<unsigned> &vals,
                          const ROOT::RVec<bool> &mask) {
      ROOT::RVec<unsigned> out;
      out.reserve(vals.size());
      for (size_t i = 0; i < vals.size(); ++i) {
        if (mask[i])
          out.push_back(vals[i]);
      }
      return out;
    };

    auto filter_costheta = [](const ROOT::RVec<float> &theta,
                              const ROOT::RVec<bool> &mask) {
      ROOT::RVec<float> out;
      out.reserve(theta.size());
      for (size_t i = 0; i < theta.size(); ++i) {
        if (mask[i])
          out.push_back(std::cos(theta[i]));
      }
      return out;
    };

    auto mu_df = df.Define("muon_trk_score_v", filter_float,
                           {"track_shower_scores", "muon_mask"})
                     .Define("muon_trk_llr_pid_v", filter_float,
                             {"trk_llr_pid_v", "muon_mask"})
                     .Define("muon_trk_start_x_v", filter_float,
                             {"track_start_x", "muon_mask"})
                     .Define("muon_trk_start_y_v", filter_float,
                             {"track_start_y", "muon_mask"})
                     .Define("muon_trk_start_z_v", filter_float,
                             {"track_start_z", "muon_mask"})
                     .Define("muon_trk_end_x_v", filter_float,
                             {"track_end_x", "muon_mask"})
                     .Define("muon_trk_end_y_v", filter_float,
                             {"track_end_y", "muon_mask"})
                     .Define("muon_trk_end_z_v", filter_float,
                             {"track_end_z", "muon_mask"})
                     .Define("muon_trk_length_v", filter_float,
                             {"track_length", "muon_mask"})
                     .Define("muon_trk_distance_v", filter_float,
                             {"track_distance_to_vertex", "muon_mask"})
                    .Define("muon_pfp_generation_v", filter_uint,
                            {"pfp_generations", "muon_mask"})
                     .Define("muon_track_costheta", filter_costheta,
                             {"track_theta", "muon_mask"})
                     .Define("n_muons_tot", "ROOT::VecOps::Sum(muon_mask)")
                     .Define("has_muon", "n_muons_tot > 0");

    return mu_df;
  }
};

} 

#endif
