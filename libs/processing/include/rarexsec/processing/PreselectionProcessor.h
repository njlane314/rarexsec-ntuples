#ifndef RAREXSEC_PROCESSING_PRESELECTION_PROCESSOR_H
#define RAREXSEC_PROCESSING_PRESELECTION_PROCESSOR_H

#include <rarexsec/processing/IEventProcessor.h>

namespace analysis {

class PreselectionProcessor : public IEventProcessor {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

}

#endif
