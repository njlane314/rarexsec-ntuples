#include <rarexsec/MuonSelectionProcessor.h>
#include <rarexsec/Catalogue.h>

#include <array>
#include <cmath>
#include <string>
#include <string_view>
#include <utility>

namespace proc {

namespace {

using MaskedColumn = std::pair<std::string_view, std::string_view>;

constexpr std::array<MaskedColumn, 10> kMuonFloatColumns = {{
    {"muon_trk_score_v", "track_shower_scores"},
    {"muon_trk_llr_pid_v", "trk_llr_pid_v"},
    {"muon_trk_start_x_v", "track_start_x"},
    {"muon_trk_start_y_v", "track_start_y"},
    {"muon_trk_start_z_v", "track_start_z"},
    {"muon_trk_end_x_v", "track_end_x"},
    {"muon_trk_end_y_v", "track_end_y"},
    {"muon_trk_end_z_v", "track_end_z"},
    {"muon_trk_length_v", "track_length"},
    {"muon_trk_distance_v", "track_distance_to_vertex"},
}};

ROOT::RVec<bool> makeMuonMask(const ROOT::RVec<float> &scores, const ROOT::RVec<float> &llr,
                              const ROOT::RVec<float> &lengths, const ROOT::RVec<float> &dists,
                              const ROOT::RVec<float> &start_x, const ROOT::RVec<float> &start_y,
                              const ROOT::RVec<float> &start_z, const ROOT::RVec<float> &end_x,
                              const ROOT::RVec<float> &end_y, const ROOT::RVec<float> &end_z,
                              const ROOT::RVec<unsigned> &gens, const ROOT::RVec<int> &plane_hits_u,
                              const ROOT::RVec<int> &plane_hits_v, const ROOT::RVec<int> &plane_hits_y) {
    const auto track_count = scores.size();
    ROOT::RVec<bool> mask;
    mask.reserve(track_count);
    for (std::size_t idx = 0; idx < track_count; ++idx) {
        mask.emplace_back(isMuonCandidate(scores[idx], llr[idx], lengths[idx], dists[idx], gens[idx], start_x[idx],
                                          start_y[idx], start_z[idx], end_x[idx], end_y[idx], end_z[idx],
                                          plane_hits_u[idx], plane_hits_v[idx], plane_hits_y[idx]));
    }
    return mask;
}

template <typename T>
ROOT::RDF::RNode defineMaskedColumn(ROOT::RDF::RNode node, std::string_view alias, std::string_view column) {
    ROOT::RDF::ColumnNames_t columns{std::string(column), "muon_mask"};
    return node.Define(std::string(alias), filterByMask<T>, columns);
}

ROOT::RVec<float> computeMaskedCosTheta(const ROOT::RVec<float> &theta, const ROOT::RVec<bool> &mask) {
    return transformByMask(theta, mask, [](float angle) { return static_cast<float>(std::cos(angle)); });
}

} // namespace

ROOT::RDF::RNode MuonSelectionProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto processed = extractMuonFeatures(buildMuonMask(std::move(df)));
    return next_ ? next_->process(std::move(processed), st) : processed;
}

ROOT::RDF::RNode MuonSelectionProcessor::buildMuonMask(ROOT::RDF::RNode df) const {
    return df.Define(
        "muon_mask",
        makeMuonMask,
        {"track_shower_scores",
         "trk_llr_pid_v",
         "track_length",
         "track_distance_to_vertex",
         "track_start_x",
         "track_start_y",
         "track_start_z",
         "track_end_x",
         "track_end_y",
         "track_end_z",
         "pfp_generations",
         "pfp_num_plane_hits_U",
         "pfp_num_plane_hits_V",
         "pfp_num_plane_hits_Y"});
}

ROOT::RDF::RNode MuonSelectionProcessor::extractMuonFeatures(ROOT::RDF::RNode df) const {
    for (const auto &[alias, column] : kMuonFloatColumns) {
        df = defineMaskedColumn<float>(df, alias, column);
    }

    df = defineMaskedColumn<unsigned>(df, "muon_pfp_generation_v", "pfp_generations");

    return df.Define("muon_track_costheta", computeMaskedCosTheta,
                     {"track_theta",
                      "muon_mask"})
             .Define("n_muons_tot", "ROOT::VecOps::Sum(muon_mask)")
             .Define("has_muon", "n_muons_tot > 0");
}

}
