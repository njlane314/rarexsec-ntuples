#ifndef RAREXSEC_WEIGHT_PROCESSOR_H
#define RAREXSEC_WEIGHT_PROCESSOR_H

#include <nlohmann/json.hpp>

#include <rarexsec/IEventProcessor.h>
#include <rarexsec/SampleTypes.h>

namespace proc {

class WeightProcessor : public IEventProcessor {
  public:
    WeightProcessor(const nlohmann::json &cfg, double total_run_pot, long total_run_triggers);

    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;

  private:
    double sample_pot_;
    long sample_triggers_;
    double total_run_pot_;
    long total_run_triggers_;
};

}

#endif
