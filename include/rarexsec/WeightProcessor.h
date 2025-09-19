#ifndef WEIGHT_PROCESSOR_H
#define WEIGHT_PROCESSOR_H

#include <rarexsec/EventProcessorStage.h>

#include "nlohmann/json.hpp"

namespace proc {

class WeightProcessor : public EventProcessorStage {
  public:
    explicit WeightProcessor(
        const nlohmann::json &sample_json,
        double total_run_pot,
        long total_run_triggers);

    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;

  private:
    double sample_pot_;
    long sample_triggers_;
    double total_run_pot_;
    long total_run_triggers_;
};

} // namespace proc

#endif
