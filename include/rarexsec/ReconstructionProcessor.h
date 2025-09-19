#ifndef RECONSTRUCTION_PROCESSOR_H
#define RECONSTRUCTION_PROCESSOR_H

#include <rarexsec/EventProcessorStage.h>

namespace proc {

class ReconstructionProcessor : public EventProcessorStage {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

} // namespace proc

#endif
