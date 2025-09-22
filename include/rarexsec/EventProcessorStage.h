#ifndef EVENT_PROCESSOR_STAGE_H
#define EVENT_PROCESSOR_STAGE_H

#include "ROOT/RDataFrame.hxx"

#include <rarexsec/SampleTypes.h>

namespace proc {

class EventProcessorStage {
  public:
    virtual ~EventProcessorStage() = default;

    virtual ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const = 0;
};

}

#endif
