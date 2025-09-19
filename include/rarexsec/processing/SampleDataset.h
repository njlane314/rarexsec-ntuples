#ifndef RAREXSEC_PROCESSING_SAMPLE_DATASET_H
#define RAREXSEC_PROCESSING_SAMPLE_DATASET_H

#include <unordered_map>

#include "ROOT/RDataFrame.hxx"

#include <rarexsec/processing/AnalysisKey.h>
#include <rarexsec/processing/SampleTypes.h>

namespace analysis {

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
