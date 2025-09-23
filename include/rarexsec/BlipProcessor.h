#ifndef BLIP_PROCESSOR_H
#define BLIP_PROCESSOR_H

#include <rarexsec/EventProcessorStage.h>

namespace proc {

class BlipProcessor : public EventProcessorStage {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, [[maybe_unused]] SampleOrigin st) const override;
};

}

#endif
