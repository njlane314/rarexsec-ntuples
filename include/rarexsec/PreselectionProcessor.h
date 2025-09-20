#ifndef PRESELECTION_PROCESSOR_H
#define PRESELECTION_PROCESSOR_H

#include <rarexsec/EventProcessorStage.h>

namespace proc {

class PreselectionProcessor : public EventProcessorStage {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

}

#endif
