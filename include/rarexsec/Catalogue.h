#ifndef SELECTION_CATALOGUE_H
#define SELECTION_CATALOGUE_H

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RVec.hxx"

#include <rarexsec/SampleTypes.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>

namespace proc {
namespace selection {

constexpr float kMinX = 5.f;
constexpr float kMaxX = 251.f;
constexpr float kMinY = -110.f;
constexpr float kMaxY = 110.f;
constexpr float kMinZ = 20.f;
constexpr float kMaxZ = 986.f;
constexpr float kGapMinZ = 675.f;
constexpr float kGapMaxZ = 775.f;

inline bool isInFiducialVolume(float x, float y, float z) {
    return x >= kMinX && x <= kMaxX && y >= kMinY && y <= kMaxY && z >= kMinZ && z <= kMaxZ;
}

inline bool isInFiducialVolumeWithGap(float x, float y, float z) {
    return isInFiducialVolume(x, y, z) && (z <= kGapMinZ || z >= kGapMaxZ);
}

inline bool passesDatasetGate(SampleOrigin origin, float pe_beam, float pe_veto, bool only_mc = false) {
    const bool gate_pass = pe_beam > 0.f && pe_veto < 20.f;
    if (only_mc) {
        return origin == SampleOrigin::kMonteCarlo ? gate_pass : true;
    }
    if (origin == SampleOrigin::kData || origin == SampleOrigin::kExternal) {
        return true;
    }
    return gate_pass;
}

inline bool isSingleGoodSlice(int num_slices, float topological_score) {
    return num_slices == 1 && topological_score > 0.06f;
}

inline bool passesSliceQuality(float contained_fraction, float cluster_fraction) {
    return contained_fraction >= 0.7f && cluster_fraction >= 0.5f;
}

template <typename PlaneHitsU, typename PlaneHitsV, typename PlaneHitsY>
inline bool passesMuonId(float score, float llr, float length, float distance_to_vertex, unsigned generation,
                         PlaneHitsU hits_u, PlaneHitsV hits_v, PlaneHitsY hits_y) {
    return score > 0.8f && llr > 0.2f && length > 10.0f && distance_to_vertex < 4.0f && generation == 2u && hits_u > 0 &&
           hits_v > 0 && hits_y > 0;
}

inline bool isMuonTrackFiducial(float start_x, float start_y, float start_z, float end_x, float end_y, float end_z) {
    return isInFiducialVolume(start_x, start_y, start_z) && isInFiducialVolume(end_x, end_y, end_z);
}

template <typename PlaneHitsU, typename PlaneHitsV, typename PlaneHitsY>
inline bool isMuonCandidate(float score, float llr, float length, float distance_to_vertex, unsigned generation,
                            float start_x, float start_y, float start_z, float end_x, float end_y, float end_z,
                            PlaneHitsU hits_u, PlaneHitsV hits_v, PlaneHitsY hits_y) {
    return passesMuonId(score, llr, length, distance_to_vertex, generation, hits_u, hits_v, hits_y) &&
           isMuonTrackFiducial(start_x, start_y, start_z, end_x, end_y, end_z);
}

inline ROOT::RDF::RNode ensureGenerationCount(ROOT::RDF::RNode df, std::string_view column, unsigned generation) {
    const std::string column_name{column};
    if (df.HasColumn(column_name)) {
        return df;
    }
    return df.Define(
        column_name,
        [generation](const ROOT::RVec<unsigned> &gens) { return ROOT::VecOps::Sum(gens == generation); },
        {"pfp_generations"});
}

inline ROOT::RDF::RNode ensureSoftwareTrigger(ROOT::RDF::RNode df, SampleOrigin origin) {
    const auto define_trigger = [&df](const char *pre, const char *post) {
        return df.Define(
            "software_trigger",
            [](unsigned run, int pre_count, int post_count) { return run < 16880 ? pre_count > 0 : post_count > 0; },
            {"run", pre, post});
    };

    if (origin == SampleOrigin::kMonteCarlo) {
        if (df.HasColumn("software_trigger_pre_ext")) {
            return define_trigger("software_trigger_pre_ext", "software_trigger_post_ext");
        }
        if (df.HasColumn("software_trigger_pre")) {
            return define_trigger("software_trigger_pre", "software_trigger_post");
        }
    }

    if (df.HasColumn("software_trigger")) {
        return df.Redefine("software_trigger", "software_trigger != 0");
    }

    return df.Define("software_trigger", []() { return true; });
}

struct Identity {
    template <typename T>
    constexpr T operator()(const T &value) const {
        return value;
    }
};

template <typename T, typename Transform>
auto transformByMask(const ROOT::RVec<T> &values, const ROOT::RVec<bool> &mask, Transform transform)
    -> ROOT::RVec<std::decay_t<std::invoke_result_t<Transform, const T &>>> {
    using Result = std::decay_t<std::invoke_result_t<Transform, const T &>>;
    ROOT::RVec<Result> out;
    out.reserve(values.size());
    const auto count = values.size();
    for (std::size_t i = 0; i < count; ++i) {
        if (mask[i]) {
            const auto &value = values[i];
            out.emplace_back(transform(value));
        }
    }
    return out;
}

template <typename T>
ROOT::RVec<T> filterByMask(const ROOT::RVec<T> &values, const ROOT::RVec<bool> &mask) {
    return transformByMask(values, mask, Identity{});
}

} // namespace selection
} // namespace proc

#endif
