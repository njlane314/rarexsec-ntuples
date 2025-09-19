#ifndef RAREXSEC_PROCESSING_IEVENT_PROCESSOR_H
#define RAREXSEC_PROCESSING_IEVENT_PROCESSOR_H

#include <memory>

#include "ROOT/RDataFrame.hxx"

#include <rarexsec/SampleTypes.h>

namespace proc {

class IEventProcessor {
  public:
    virtual ~IEventProcessor() = default;

    virtual ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const = 0;

    void chainNextProcessor(std::unique_ptr<IEventProcessor> next) { next_ = std::move(next); }

  protected:
    std::unique_ptr<IEventProcessor> next_;
};

}

#endif
