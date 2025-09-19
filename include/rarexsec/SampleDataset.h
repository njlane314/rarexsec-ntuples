#ifndef SAMPLE_DATASET_H
#define SAMPLE_DATASET_H

#include <unordered_map>

#include "ROOT/RDataFrame.hxx"

#include <rarexsec/AnalysisKey.h>
#include <rarexsec/SampleTypes.h>

namespace proc {

struct SampleDataset {
    SampleOrigin origin_;
    AnalysisRole role_;
    mutable ROOT::RDF::RNode dataframe_;
};

struct SampleDatasetGroup {
    SampleDataset nominal_;
    std::unordered_map<SampleVariation, SampleDataset> variations_;
};

using SampleDatasetGroupMap = std::unordered_map<SampleKey, SampleDatasetGroup>;

}

#endif
