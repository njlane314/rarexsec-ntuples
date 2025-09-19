#ifndef RAREXSEC_PROCESSING_RECONSTRUCTION_PROCESSOR_H
#define RAREXSEC_PROCESSING_RECONSTRUCTION_PROCESSOR_H

#include "ROOT/RVec.hxx"

#include <rarexsec/IEventProcessor.h>

namespace proc {

class ReconstructionProcessor : public IEventProcessor {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

}

#endif
