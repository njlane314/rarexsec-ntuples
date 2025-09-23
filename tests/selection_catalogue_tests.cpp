#include <rarexsec/SelectionCatalogue.h>

#include <array>
#include <cassert>

using proc::SampleOrigin;

namespace {

struct DatasetGateTestCase {
    SampleOrigin origin;
    float pe_beam;
    float pe_veto;
    bool software_trigger;
    bool only_mc;
};

struct QualityTestCase {
    SampleOrigin origin;
    float pe_beam;
    float pe_veto;
    bool software_trigger;
    int num_slices;
    float topological_score;
    float vertex_x;
    float vertex_y;
    float vertex_z;
    float contained_fraction;
    float slice_cluster_fraction;
    bool only_mc;
    bool require_trigger;
};

} // namespace

void testPassesDatasetGateWithTrigger() {
    const std::array<DatasetGateTestCase, 8> cases{{
        DatasetGateTestCase{SampleOrigin::kMonteCarlo, 10.f, 5.f, true, false},
        DatasetGateTestCase{SampleOrigin::kMonteCarlo, 10.f, 25.f, true, false},
        DatasetGateTestCase{SampleOrigin::kMonteCarlo, 10.f, 5.f, false, false},
        DatasetGateTestCase{SampleOrigin::kData, 0.f, 30.f, true, false},
        DatasetGateTestCase{SampleOrigin::kData, 0.f, 30.f, false, false},
        DatasetGateTestCase{SampleOrigin::kMonteCarlo, 10.f, 5.f, true, true},
        DatasetGateTestCase{SampleOrigin::kMonteCarlo, 0.f, 10.f, true, true},
        DatasetGateTestCase{SampleOrigin::kData, 0.f, 30.f, false, true},
    }};

    for (const auto &test_case : cases) {
        const bool expected = proc::selc::passesDatasetGate(test_case.origin,
                                                            test_case.pe_beam,
                                                            test_case.pe_veto,
                                                            test_case.only_mc) &&
                              test_case.software_trigger;
        const bool actual = proc::selc::passesDatasetGateWithTrigger(test_case.origin,
                                                                     test_case.pe_beam,
                                                                     test_case.pe_veto,
                                                                     test_case.software_trigger,
                                                                     test_case.only_mc);
        assert(actual == expected);
    }
}

void testPassesQualityCuts() {
    const std::array<QualityTestCase, 10> cases{{
        // Preselection-style requirements: dataset gate + trigger
        QualityTestCase{SampleOrigin::kMonteCarlo, 10.f, 5.f, true, 1, 0.1f, 100.f, 0.f, 650.f, 0.9f, 0.7f, false, true},
        QualityTestCase{SampleOrigin::kMonteCarlo, 10.f, 25.f, true, 1, 0.1f, 100.f, 0.f, 650.f, 0.9f, 0.7f, false, true},
        QualityTestCase{SampleOrigin::kData, 0.f, 30.f, false, 1, 0.1f, 100.f, 0.f, 650.f, 0.9f, 0.7f, false, true},
        QualityTestCase{SampleOrigin::kData, 5.f, 10.f, true, 2, 0.1f, 100.f, 0.f, 650.f, 0.9f, 0.7f, false, true},
        QualityTestCase{SampleOrigin::kData, 5.f, 10.f, true, 1, 0.04f, 100.f, 0.f, 650.f, 0.9f, 0.7f, false, true},
        // Reconstruction-style requirements: dataset gate for MC only, ignore trigger
        QualityTestCase{SampleOrigin::kMonteCarlo, 10.f, 5.f, true, 1, 0.1f, 100.f, 0.f, 650.f, 0.9f, 0.7f, true, false},
        QualityTestCase{SampleOrigin::kMonteCarlo, 0.f, 5.f, true, 1, 0.1f, 100.f, 0.f, 650.f, 0.9f, 0.7f, true, false},
        QualityTestCase{SampleOrigin::kData, 0.f, 30.f, false, 1, 0.1f, 100.f, 0.f, 700.f, 0.9f, 0.7f, true, false},
        QualityTestCase{SampleOrigin::kData, 5.f, 10.f, false, 1, 0.1f, 100.f, 0.f, 650.f, 0.6f, 0.4f, true, false},
        QualityTestCase{SampleOrigin::kData, 5.f, 10.f, false, 1, 0.1f, 100.f, 0.f, 650.f, 0.9f, 0.7f, true, false},
    }};

    for (const auto &test_case : cases) {
        const bool dataset_gate = test_case.require_trigger
                                      ? proc::selc::passesDatasetGate(test_case.origin,
                                                                      test_case.pe_beam,
                                                                      test_case.pe_veto,
                                                                      test_case.only_mc) &&
                                            test_case.software_trigger
                                      : proc::selc::passesDatasetGate(test_case.origin,
                                                                      test_case.pe_beam,
                                                                      test_case.pe_veto,
                                                                      test_case.only_mc);
        const bool expected = dataset_gate &&
                              proc::selc::isSingleGoodSlice(test_case.num_slices, test_case.topological_score) &&
                              proc::selc::isInFiducialVolumeWithGap(test_case.vertex_x,
                                                                     test_case.vertex_y,
                                                                     test_case.vertex_z) &&
                              proc::selc::passesSliceQuality(test_case.contained_fraction,
                                                              test_case.slice_cluster_fraction);
        const bool actual = proc::selc::passesQualityCuts(test_case.origin,
                                                          test_case.pe_beam,
                                                          test_case.pe_veto,
                                                          test_case.software_trigger,
                                                          test_case.num_slices,
                                                          test_case.topological_score,
                                                          test_case.vertex_x,
                                                          test_case.vertex_y,
                                                          test_case.vertex_z,
                                                          test_case.contained_fraction,
                                                          test_case.slice_cluster_fraction,
                                                          test_case.only_mc,
                                                          test_case.require_trigger);
        assert(actual == expected);
    }
}

int main() {
    testPassesDatasetGateWithTrigger();
    testPassesQualityCuts();
    return 0;
}
