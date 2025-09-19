#ifndef RAREXSEC_PROCESSING_NUMU_CC_SELECTION_PROCESSOR_H
#define RAREXSEC_PROCESSING_NUMU_CC_SELECTION_PROCESSOR_H

#include <rarexsec/processing/IEventProcessor.h>

namespace proc {

class NuMuCCSelectionProcessor : public IEventProcessor {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

}

#endif
