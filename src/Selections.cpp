#include <rarexsec/Selections.h>

namespace proc {
namespace selection {

FilterExpression muonNeutrinoChargedCurrentSelection() {
    return FilterExpression{"pass_pre && pass_flash && pass_fv && pass_topo && has_muon"};
}

} // namespace selection
} // namespace proc
