#ifndef RAREXSEC_PROCESSING_BLIP_PROCESSOR_H
#define RAREXSEC_PROCESSING_BLIP_PROCESSOR_H

#include <string>

#include "ROOT/RVec.hxx"

#include <rarexsec/processing/IEventProcessor.h>

namespace analysis {

class BlipProcessor : public IEventProcessor {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

}

#endif
