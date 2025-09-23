#include <gtest/gtest.h>

#include <rarexsec/SampleTypes.h>
#include <rarexsec/WeightProcessor.h>

#include <ROOT/RDataFrame.hxx>

#include <nlohmann/json.hpp>

#include <limits>
#include <vector>

using proc::SampleOrigin;

namespace {

nlohmann::json makeSampleConfig(double pot, long triggers) {
    nlohmann::json cfg;
    cfg["pot"] = pot;
    cfg["triggers"] = triggers;
    return cfg;
}

class WeightProcessorSimulationTest : public ::testing::TestWithParam<SampleOrigin> {
  protected:
    static ROOT::RDF::RNode makeInput() {
        ROOT::RDataFrame df(3);
        return df.Define(
                     "base_event_weight",
                     [](ULong64_t entry) { return static_cast<double>(entry + 1); },
                     {"rdfentry_"})
            .Define(
                "weightSpline",
                [](ULong64_t entry) {
                    if (entry == 0) {
                        return 2.0f;
                    }
                    if (entry == 1) {
                        return std::numeric_limits<float>::quiet_NaN();
                    }
                    return 2.0f;
                },
                {"rdfentry_"})
            .Define(
                "weightTune",
                [](ULong64_t entry) {
                    if (entry == 0) {
                        return 0.5f;
                    }
                    if (entry == 1) {
                        return 1.0f;
                    }
                    return std::numeric_limits<float>::infinity();
                },
                {"rdfentry_"});
    }
};

TEST_P(WeightProcessorSimulationTest, ScalesBaseWeightAndDefinesNominalWeight) {
    const double sample_pot = 1.0;
    const double total_pot = 2.0;
    const long total_triggers = 0;
    WeightProcessor processor(makeSampleConfig(sample_pot, 0), total_pot, total_triggers);

    auto processed = processor.process(makeInput(), GetParam());

    const auto base_weights = processed.Take<double>("base_event_weight");
    ASSERT_EQ(base_weights->size(), 3);
    EXPECT_DOUBLE_EQ(base_weights->at(0), 2.0);
    EXPECT_DOUBLE_EQ(base_weights->at(1), 4.0);
    EXPECT_DOUBLE_EQ(base_weights->at(2), 6.0);

    const auto nominal_weights = processed.Take<double>("nominal_event_weight");
    ASSERT_EQ(nominal_weights->size(), 3);
    EXPECT_DOUBLE_EQ(nominal_weights->at(0), 2.0);
    EXPECT_DOUBLE_EQ(nominal_weights->at(1), 4.0);
    EXPECT_DOUBLE_EQ(nominal_weights->at(2), 1.0);
}

INSTANTIATE_TEST_SUITE_P(
    SimulationOrigins,
    WeightProcessorSimulationTest,
    ::testing::Values(SampleOrigin::kMonteCarlo, SampleOrigin::kDirt));

TEST(WeightProcessorTest, ExternalDataDefinesWeightsWhenMissing) {
    const double total_pot = 0.0;
    const long total_triggers = 50;
    const long sample_triggers = 20;
    WeightProcessor processor(makeSampleConfig(0.0, sample_triggers), total_pot, total_triggers);

    ROOT::RDataFrame df(2);
    auto processed = processor.process(df, SampleOrigin::kExternal);

    const auto base_weights = processed.Take<double>("base_event_weight");
    ASSERT_EQ(base_weights->size(), 2);
    EXPECT_DOUBLE_EQ(base_weights->at(0), 2.5);
    EXPECT_DOUBLE_EQ(base_weights->at(1), 2.5);

    const auto nominal_weights = processed.Take<double>("nominal_event_weight");
    ASSERT_EQ(nominal_weights->size(), 2);
    EXPECT_DOUBLE_EQ(nominal_weights->at(0), 2.5);
    EXPECT_DOUBLE_EQ(nominal_weights->at(1), 2.5);
}

} // namespace
