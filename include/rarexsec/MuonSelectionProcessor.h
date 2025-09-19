#ifndef RAREXSEC_PROCESSING_MUON_SELECTION_PROCESSOR_H
#define RAREXSEC_PROCESSING_MUON_SELECTION_PROCESSOR_H

#include <cmath>

#include "ROOT/RVec.hxx"

#include <rarexsec/IEventProcessor.h>

namespace proc {

class MuonSelectionProcessor : public IEventProcessor {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override;

  private:
    ROOT::RDF::RNode buildMuonMask(ROOT::RDF::RNode df) const;
    ROOT::RDF::RNode extractMuonFeatures(ROOT::RDF::RNode df) const;
};

}

#endif
