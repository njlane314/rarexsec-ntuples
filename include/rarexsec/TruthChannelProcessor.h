#ifndef RAREXSEC_TRUTH_CHANNEL_PROCESSOR_H
#define RAREXSEC_TRUTH_CHANNEL_PROCESSOR_H

#include <map>
#include <mutex>

#include <rarexsec/IEventProcessor.h>

namespace proc {

class TruthChannelProcessor : public IEventProcessor {
  public:
    TruthChannelProcessor() = default;
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;

  private:
    ROOT::RDF::RNode processNonMc(ROOT::RDF::RNode df, SampleOrigin st) const;
    ROOT::RDF::RNode defineCounts(ROOT::RDF::RNode df) const;
    ROOT::RDF::RNode assignInclusiveChannels(ROOT::RDF::RNode df) const;
    ROOT::RDF::RNode assignExclusiveChannels(ROOT::RDF::RNode df) const;
    ROOT::RDF::RNode assignChannelDefinitions(ROOT::RDF::RNode df) const;
};

}

#endif
