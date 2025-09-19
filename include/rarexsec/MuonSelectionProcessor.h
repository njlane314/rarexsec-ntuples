#ifndef MUON_SELECTION_PROCESSOR_H
#define MUON_SELECTION_PROCESSOR_H

#include <rarexsec/EventProcessorStage.h>

namespace proc {

class MuonSelectionProcessor : public EventProcessorStage {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;
};

} // namespace proc

#endif
