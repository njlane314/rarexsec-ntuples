#ifndef TRUTH_CHANNEL_PROCESSOR_H
#define TRUTH_CHANNEL_PROCESSOR_H

#include <rarexsec/EventProcessorStage.h>

namespace proc {

class TruthChannelProcessor : public EventProcessorStage {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

} // namespace proc

#endif
