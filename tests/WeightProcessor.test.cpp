#include <rarexsec/SampleTypes.h>
#include <rarexsec/WeightProcessor.h>

#include <ROOT/RDataFrame.hxx>

#include <nlohmann/json.hpp>

#include <cmath>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

using proc::SampleOrigin;

namespace {

nlohmann::json makeSampleConfig(double pot, long triggers) {
    nlohmann::json cfg;
    cfg["pot"] = pot;
    cfg["triggers"] = triggers;
    return cfg;
}

ROOT::RDF::RNode makeInput() {
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

void check(bool condition, const std::string& message, int& failures) {
    if (!condition) {
        ++failures;
        std::cerr << "Test failure: " << message << '\n';
    }
}

void checkEqual(double actual, double expected, const std::string& message, int& failures,
                double tolerance = 1e-12) {
    if (!(std::isfinite(actual) && std::isfinite(expected) && std::abs(actual - expected) <= tolerance)) {
        std::ostringstream oss;
        oss << message << " (expected " << expected << ", got " << actual << ")";
        check(false, oss.str(), failures);
    }
}

} // namespace

int main() {
    int failures = 0;

    const double sample_pot = 1.0;
    const double total_pot = 2.0;
    const long total_triggers = 0;

    for (auto origin : {SampleOrigin::kMonteCarlo, SampleOrigin::kDirt}) {
        WeightProcessor processor(makeSampleConfig(sample_pot, 0), total_pot, total_triggers);
        auto processed = processor.process(makeInput(), origin);

        const auto base_weights = processed.Take<double>("base_event_weight");
        check(base_weights && base_weights->size() == 3, "Simulation base weights size", failures);
        if (base_weights && base_weights->size() == 3) {
            checkEqual(base_weights->at(0), 2.0, "Simulation base weight entry 0", failures);
            checkEqual(base_weights->at(1), 4.0, "Simulation base weight entry 1", failures);
            checkEqual(base_weights->at(2), 6.0, "Simulation base weight entry 2", failures);
        }

        const auto nominal_weights = processed.Take<double>("nominal_event_weight");
        check(nominal_weights && nominal_weights->size() == 3, "Simulation nominal weights size", failures);
        if (nominal_weights && nominal_weights->size() == 3) {
            checkEqual(nominal_weights->at(0), 2.0, "Simulation nominal weight entry 0", failures);
            checkEqual(nominal_weights->at(1), 4.0, "Simulation nominal weight entry 1", failures);
            checkEqual(nominal_weights->at(2), 1.0, "Simulation nominal weight entry 2", failures);
        }
    }

    const double total_external_pot = 0.0;
    const long total_external_triggers = 50;
    const long sample_triggers = 20;

    WeightProcessor processor(makeSampleConfig(0.0, sample_triggers), total_external_pot, total_external_triggers);
    ROOT::RDataFrame df(2);
    auto processed = processor.process(df, SampleOrigin::kExternal);

    const auto base_weights = processed.Take<double>("base_event_weight");
    check(base_weights && base_weights->size() == 2, "External base weights size", failures);
    if (base_weights && base_weights->size() == 2) {
        checkEqual(base_weights->at(0), 2.5, "External base weight entry 0", failures);
        checkEqual(base_weights->at(1), 2.5, "External base weight entry 1", failures);
    }

    const auto nominal_weights = processed.Take<double>("nominal_event_weight");
    check(nominal_weights && nominal_weights->size() == 2, "External nominal weights size", failures);
    if (nominal_weights && nominal_weights->size() == 2) {
        checkEqual(nominal_weights->at(0), 2.5, "External nominal weight entry 0", failures);
        checkEqual(nominal_weights->at(1), 2.5, "External nominal weight entry 1", failures);
    }

    if (failures > 0) {
        std::cerr << failures << " test" << (failures == 1 ? "" : "s") << " failed." << std::endl;
        return 1;
    }

    std::cout << "All tests passed." << std::endl;
    return 0;
}
