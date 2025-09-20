#ifndef TRUTH_CHANNEL_PROCESSOR_H
#define TRUTH_CHANNEL_PROCESSOR_H

#include <rarexsec/EventProcessorStage.h>

namespace proc {

class TruthChannelProcessor : public EventProcessorStage {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;

  private:
    ROOT::RDF::RNode processData(ROOT::RDF::RNode df, SampleOrigin st) const;
    ROOT::RDF::RNode defineCounts(ROOT::RDF::RNode df) const;
    ROOT::RDF::RNode assignInclusiveChannels(ROOT::RDF::RNode df) const;
    ROOT::RDF::RNode assignExclusiveChannels(ROOT::RDF::RNode df) const;
    ROOT::RDF::RNode assignChannelDefinitions(ROOT::RDF::RNode df) const;
};

}

#endif
