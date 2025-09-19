#ifndef RAREXSEC_BLIP_PROCESSOR_H
#define RAREXSEC_BLIP_PROCESSOR_H

#include <string>

#include "ROOT/RVec.hxx"

#include <rarexsec/IEventProcessor.h>

namespace proc {

class BlipProcessor : public IEventProcessor {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

}

#endif
