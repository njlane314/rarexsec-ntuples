#ifndef WEIGHT_PROCESSOR_H
#define WEIGHT_PROCESSOR_H

#include <rarexsec/EventProcessorStage.h>

#include "nlohmann/json.hpp"

namespace proc {

class WeightProcessor : public EventProcessorStage {
  public:
    explicit WeightProcessor(const nlohmann::json &sample_json, double total_pot, long total_triggers);

    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;

  private:
    double total_pot_;
    long total_triggers_;
};

} // namespace proc

#endif
