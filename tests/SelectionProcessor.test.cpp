#include <gtest/gtest.h>

#include <rarexsec/PreselectionProcessor.h>
#include <rarexsec/ReconstructionProcessor.h>
#include <rarexsec/SampleTypes.h>

#include <ROOT/RDataFrame.hxx>

#include <array>
#include <vector>

using proc::SampleOrigin;

namespace {

constexpr std::array<float, 5> kPeBeam{10.f, 0.f, 10.f, 10.f, 10.f};
constexpr std::array<float, 5> kPeVeto{5.f, 5.f, 5.f, 5.f, 5.f};
constexpr std::array<int, 5> kSoftwareTrigger{1, 1, 0, 1, 1};
constexpr std::array<int, 5> kNumSlices{1, 1, 1, 2, 1};
constexpr std::array<float, 5> kTopologicalScore{0.1f, 0.1f, 0.1f, 0.1f, 0.1f};
constexpr std::array<float, 5> kVertexX{50.f, 50.f, 50.f, 50.f, 50.f};
constexpr std::array<float, 5> kVertexY{0.f, 0.f, 0.f, 0.f, 0.f};
constexpr std::array<float, 5> kVertexZ{500.f, 500.f, 500.f, 500.f, 500.f};
constexpr std::array<float, 5> kContainedFraction{0.8f, 0.8f, 0.8f, 0.8f, 0.8f};
constexpr std::array<float, 5> kClusterFraction{0.6f, 0.6f, 0.6f, 0.6f, 0.4f};
constexpr std::array<int, 5> kMuonCounts{1, 1, 1, 1, 0};

ROOT::RDF::RNode makeSelectionInput() {
    ROOT::RDataFrame df(kPeBeam.size());
    return df.Define("optical_filter_pe_beam",
                     [](ULong64_t entry) { return kPeBeam[entry]; },
                     {"rdfentry_"})
        .Define("optical_filter_pe_veto",
                [](ULong64_t entry) { return kPeVeto[entry]; },
                {"rdfentry_"})
        .Define("software_trigger",
                [](ULong64_t entry) { return kSoftwareTrigger[entry]; },
                {"rdfentry_"})
        .Define("num_slices",
                [](ULong64_t entry) { return kNumSlices[entry]; },
                {"rdfentry_"})
        .Define("topological_score",
                [](ULong64_t entry) { return kTopologicalScore[entry]; },
                {"rdfentry_"})
        .Define("reco_neutrino_vertex_sce_x",
                [](ULong64_t entry) { return kVertexX[entry]; },
                {"rdfentry_"})
        .Define("reco_neutrino_vertex_sce_y",
                [](ULong64_t entry) { return kVertexY[entry]; },
                {"rdfentry_"})
        .Define("reco_neutrino_vertex_sce_z",
                [](ULong64_t entry) { return kVertexZ[entry]; },
                {"rdfentry_"})
        .Define("contained_fraction",
                [](ULong64_t entry) { return kContainedFraction[entry]; },
                {"rdfentry_"})
        .Define("slice_cluster_fraction",
                [](ULong64_t entry) { return kClusterFraction[entry]; },
                {"rdfentry_"})
        .Define("n_muons_tot",
                [](ULong64_t entry) { return kMuonCounts[entry]; },
                {"rdfentry_"})
        .Define("n_pfps_gen2", []() { return 0u; })
        .Define("n_pfps_gen3", []() { return 0u; });
}

} // namespace

TEST(SelectionProcessorTest, PreselectionAppliesCompositeSelections) {
    proc::PreselectionProcessor processor;
    auto processed = processor.process(makeSelectionInput(), SampleOrigin::kMonteCarlo);

    const auto pass_pre = processed.Take<bool>("pass_pre");
    const std::vector<bool> expected_pre{true, false, false, true, true};
    ASSERT_EQ(pass_pre->size(), expected_pre.size());
    for (std::size_t i = 0; i < expected_pre.size(); ++i) {
        EXPECT_EQ(pass_pre->at(i), expected_pre[i]) << "Entry " << i;
    }

    const auto pass_final = processed.Take<bool>("pass_final");
    const std::vector<bool> expected_final{true, false, false, false, false};
    ASSERT_EQ(pass_final->size(), expected_final.size());
    for (std::size_t i = 0; i < expected_final.size(); ++i) {
        EXPECT_EQ(pass_final->at(i), expected_final[i]) << "Entry " << i;
    }
}

TEST(SelectionProcessorTest, ReconstructionAppliesQualityCutsForSimulation) {
    proc::ReconstructionProcessor processor;
    auto processed = processor.process(makeSelectionInput(), SampleOrigin::kMonteCarlo);

    const auto quality_event = processed.Take<bool>("quality_event");
    const std::vector<bool> expected{true, false, false, false, false};
    ASSERT_EQ(quality_event->size(), expected.size());
    for (std::size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(quality_event->at(i), expected[i]) << "Entry " << i;
    }
}

TEST(SelectionProcessorTest, ReconstructionKeepsDataWithoutDatasetGate) {
    proc::ReconstructionProcessor processor;
    auto processed = processor.process(makeSelectionInput(), SampleOrigin::kData);

    const auto quality_event = processed.Take<bool>("quality_event");
    const std::vector<bool> expected{true, true, false, false, false};
    ASSERT_EQ(quality_event->size(), expected.size());
    for (std::size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(quality_event->at(i), expected[i]) << "Entry " << i;
    }
}
