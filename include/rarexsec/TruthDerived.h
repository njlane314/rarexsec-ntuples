#ifndef RAREXSEC_TRUTH_DERIVED_H
#define RAREXSEC_TRUTH_DERIVED_H

namespace proc {

struct TruthDerived {
    bool in_fiducial;
    int mc_n_strange;
    int mc_n_pion;
    int mc_n_proton;
    int interaction_mode_category;
    int inclusive_strange_channel_category;
    int exclusive_strange_channel_category;
    int channel_definition_category;
    bool is_truth_signal;
    bool pure_slice_signal;
};

} // namespace proc

#endif // RAREXSEC_TRUTH_DERIVED_H
