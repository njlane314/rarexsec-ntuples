#include <rarexsec/TruthChannelProcessor.h>
#include <rarexsec/SelectionCatalogue.h>

#include <cmath>

namespace proc {
namespace {

struct DataSampleChannelInfo {
    int channel;
    int definition;
};

DataSampleChannelInfo channelInfoForDataSample(SampleOrigin origin) {
    switch (origin) {
    case SampleOrigin::kData:
        return {0, 0};
    case SampleOrigin::kExternal:
        return {1, 1};
    case SampleOrigin::kDirt:
        return {2, 1};
    default:
        return {99, 99};
    }
}

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

inline int to_mode_cat(int mode) {
    switch (mode) {
    case 0:
        return 0;
    case 1:
        return 1;
    case 2:
        return 2;
    case 3:
        return 3;
    case 10:
        return 10;
    default:
        return -1;
    }
}

}

ROOT::RDF::RNode TruthChannelProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    if (st != SampleOrigin::kMonteCarlo) {
        return processData(df, st);
    }

    auto with_truth = df.Define(
        "truth_derived",
        [](float x,
           float y,
           float z,
           int mode,
           int kp,
           int km,
           int k0,
           int lam,
           int sp,
           int s0,
           int sm,
           int pip,
           int pim,
           int pi0,
           int p,
           int g,
           int nu,
           int ccnc,
           float purity,
           float completeness) {
            TruthDerived out{};
            out.in_fiducial = selc::isInFiducialVolume(x, y, z);
            out.mc_n_strange = kp + km + k0 + lam + sp + s0 + sm;
            out.mc_n_pion = pip + pim;
            out.mc_n_proton = p;
            out.interaction_mode_category = to_mode_cat(mode);

            if (!out.in_fiducial) {
                out.inclusive_strange_channel_category = 98;
            } else if (ccnc == 1) {
                out.inclusive_strange_channel_category = 31;
            } else if (std::abs(nu) == 12 && ccnc == 0) {
                out.inclusive_strange_channel_category = 30;
            } else if (std::abs(nu) == 14 && ccnc == 0) {
                if (out.mc_n_strange == 1) {
                    out.inclusive_strange_channel_category = 10;
                } else if (out.mc_n_strange > 1) {
                    out.inclusive_strange_channel_category = 11;
                } else if (out.mc_n_proton >= 1 && out.mc_n_pion == 0) {
                    out.inclusive_strange_channel_category = 20;
                } else if (out.mc_n_proton == 0 && out.mc_n_pion >= 1) {
                    out.inclusive_strange_channel_category = 21;
                } else if (out.mc_n_proton >= 1 && out.mc_n_pion >= 1) {
                    out.inclusive_strange_channel_category = 22;
                } else {
                    out.inclusive_strange_channel_category = 23;
                }
            } else {
                out.inclusive_strange_channel_category = 99;
            }

            if (!out.in_fiducial) {
                out.exclusive_strange_channel_category = 98;
            } else if (ccnc == 1) {
                out.exclusive_strange_channel_category = 31;
            } else if (std::abs(nu) == 12 && ccnc == 0) {
                out.exclusive_strange_channel_category = 30;
            } else if (std::abs(nu) == 14 && ccnc == 0) {
                const int s = out.mc_n_strange;
                if (s == 0) {
                    out.exclusive_strange_channel_category = 32;
                } else if ((kp == 1 || km == 1) && s == 1) {
                    out.exclusive_strange_channel_category = 50;
                } else if (k0 == 1 && s == 1) {
                    out.exclusive_strange_channel_category = 51;
                } else if (lam == 1 && s == 1) {
                    out.exclusive_strange_channel_category = 52;
                } else if ((sp == 1 || sm == 1) && s == 1) {
                    out.exclusive_strange_channel_category = 53;
                } else {
                    out.exclusive_strange_channel_category = 61;
                }
            } else {
                out.exclusive_strange_channel_category = 99;
            }

            if (!out.in_fiducial) {
                out.channel_definition_category = (nu == 0) ? 1 : 2;
            } else if (ccnc == 1) {
                out.channel_definition_category = 14;
            } else if (ccnc == 0 && out.mc_n_strange > 0) {
                out.channel_definition_category = (out.mc_n_strange == 1) ? 15 : 16;
            } else if (std::abs(nu) == 12 && ccnc == 0) {
                out.channel_definition_category = 17;
            } else if (std::abs(nu) == 14 && ccnc == 0) {
                if (out.mc_n_pion == 0 && out.mc_n_proton > 0) {
                    out.channel_definition_category = 10;
                } else if (out.mc_n_pion == 1 && pi0 == 0) {
                    out.channel_definition_category = 11;
                } else if (pi0 > 0 || g >= 2) {
                    out.channel_definition_category = 12;
                } else if (out.mc_n_pion > 1) {
                    out.channel_definition_category = 13;
                } else {
                    out.channel_definition_category = 18;
                }
            } else {
                out.channel_definition_category = 99;
            }

            out.is_truth_signal =
                (out.channel_definition_category == 15 || out.channel_definition_category == 16);
            out.pure_slice_signal =
                out.is_truth_signal && (purity > 0.5f && completeness > 0.1f);
            return out;
        },
        {"neutrino_vertex_x",
         "neutrino_vertex_y",
         "neutrino_vertex_z",
         "interaction_mode",
         "count_kaon_plus",
         "count_kaon_minus",
         "count_kaon_zero",
         "count_lambda",
         "count_sigma_plus",
         "count_sigma_zero",
         "count_sigma_minus",
         "count_pi_plus",
         "count_pi_minus",
         "count_pi_zero",
         "count_proton",
         "count_gamma",
         "neutrino_pdg",
         "interaction_ccnc",
         "neutrino_purity_from_pfp",
         "neutrino_completeness_from_pfp"});

    auto out = with_truth.Define("in_fiducial", "truth_derived.in_fiducial")
                    .Define("mc_n_strange", "truth_derived.mc_n_strange")
                    .Define("mc_n_pion", "truth_derived.mc_n_pion")
                    .Define("mc_n_proton", "truth_derived.mc_n_proton")
                    .Define("interaction_mode_category", "truth_derived.interaction_mode_category")
                    .Define("inclusive_strange_channel_category",
                            "truth_derived.inclusive_strange_channel_category")
                    .Define("exclusive_strange_channel_category",
                            "truth_derived.exclusive_strange_channel_category")
                    .Define("channel_definition_category",
                            "truth_derived.channel_definition_category")
                    .Define("is_truth_signal", "truth_derived.is_truth_signal")
                    .Define("pure_slice_signal", "truth_derived.pure_slice_signal");

    return next_ ? next_->process(out, st) : out;
}

ROOT::RDF::RNode TruthChannelProcessor::processData(ROOT::RDF::RNode df, SampleOrigin st) const {
    const auto [channel, channel_def] = channelInfoForDataSample(st);

    auto mode_df = df.Define("interaction_mode_category", []() { return -1; });

    auto channels_df =
        mode_df.Define("inclusive_strange_channel_category", [channel]() { return channel; })
            .Define("exclusive_strange_channel_category", [channel]() { return channel; })
            .Define("channel_definition_category", [channel_def]() { return channel_def; });

    auto defaults_df = channels_df.Define("in_fiducial", []() { return false; })
                           .Define("mc_n_strange", []() { return 0; })
                           .Define("mc_n_pion", []() { return 0; })
                           .Define("mc_n_proton", []() { return 0; })
                           .Define("is_truth_signal", []() { return false; })
                           .Define("pure_slice_signal", []() { return false; });

    return next_ ? next_->process(defaults_df, st) : defaults_df;
}

}
