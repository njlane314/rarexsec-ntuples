#ifndef RAREXSEC_PRESELECTION_PROCESSOR_H
#define RAREXSEC_PRESELECTION_PROCESSOR_H

#include <rarexsec/IEventProcessor.h>

namespace proc {

class PreselectionProcessor : public IEventProcessor {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

}

#endif
