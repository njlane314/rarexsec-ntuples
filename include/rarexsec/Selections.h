#ifndef RAREXSEC_SELECTIONS_H
#define RAREXSEC_SELECTIONS_H

#include <rarexsec/AnalysisDataLoader.h>
#include <rarexsec/SelectionQuery.h>

namespace proc {
namespace selection {

SelectionQuery muonNeutrinoCCSelection(const AnalysisDataLoader::SampleFrameMap &frames);

} // namespace selection
} // namespace proc

#endif
