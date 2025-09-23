#include <rarexsec/TruthChannelProcessor.h>
#include <rarexsec/SelectionCatalogue.h>
#include <rarexsec/TruthDerived.h>

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

int inclusiveChannelFor(const TruthDerived &truth, int nu, int ccnc) {
    if (!truth.in_fiducial) {
        return 98;
    }

    if (ccnc == 1) {
        return 31;
    }

    if (std::abs(nu) == 12 && ccnc == 0) {
        return 30;
    }

    if (std::abs(nu) == 14 && ccnc == 0) {
        if (truth.mc_n_strange == 1) {
            return 10;
        }

        if (truth.mc_n_strange > 1) {
            return 11;
        }

        if (truth.mc_n_proton >= 1 && truth.mc_n_pion == 0) {
            return 20;
        }

        if (truth.mc_n_proton == 0 && truth.mc_n_pion >= 1) {
            return 21;
        }

        if (truth.mc_n_proton >= 1 && truth.mc_n_pion >= 1) {
            return 22;
        }

        return 23;
    }

    return 99;
}

int exclusiveChannelFor(const TruthDerived &truth,
                        int kp,
                        int km,
                        int k0,
                        int lam,
                        int sp,
                        int s0,
                        int sm,
                        int nu,
                        int ccnc) {
    if (!truth.in_fiducial) {
        return 98;
    }

    if (ccnc == 1) {
        return 31;
    }

    if (std::abs(nu) == 12 && ccnc == 0) {
        return 30;
    }

    if (std::abs(nu) == 14 && ccnc == 0) {
        const int s = truth.mc_n_strange;
        if (s == 0) {
            return 32;
        }

        if ((kp == 1 || km == 1) && s == 1) {
            return 50;
        }

        if (k0 == 1 && s == 1) {
            return 51;
        }

        if (lam == 1 && s == 1) {
            return 52;
        }

        if ((sp == 1 || sm == 1) && s == 1) {
            return 53;
        }

        return 61;
    }

    return 99;
}

int channelDefinitionFor(const TruthDerived &truth, int pi0, int g, int nu, int ccnc) {
    if (!truth.in_fiducial) {
        return (nu == 0) ? 1 : 2;
    }

    if (ccnc == 1) {
        return 14;
    }

    if (ccnc == 0 && truth.mc_n_strange > 0) {
        return (truth.mc_n_strange == 1) ? 15 : 16;
    }

    if (std::abs(nu) == 12 && ccnc == 0) {
        return 17;
    }

    if (std::abs(nu) == 14 && ccnc == 0) {
        if (truth.mc_n_pion == 0 && truth.mc_n_proton > 0) {
            return 10;
        }

        if (truth.mc_n_pion == 1 && pi0 == 0) {
            return 11;
        }

        if (pi0 > 0 || g >= 2) {
            return 12;
        }

        if (truth.mc_n_pion > 1) {
            return 13;
        }

        return 18;
    }

    return 99;
}

TruthDerived buildTruthDerived(float x,
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

    out.inclusive_strange_channel_category = inclusiveChannelFor(out, nu, ccnc);
    out.exclusive_strange_channel_category =
        exclusiveChannelFor(out, kp, km, k0, lam, sp, s0, sm, nu, ccnc);
    out.channel_definition_category = channelDefinitionFor(out, pi0, g, nu, ccnc);

    out.is_truth_signal =
        (out.channel_definition_category == 15 || out.channel_definition_category == 16);
    out.pure_slice_signal = out.is_truth_signal && (purity > 0.5f && completeness > 0.1f);

    return out;
}

TruthDerived buildSyntheticTruthDerived(int channel, int channel_definition) {
    TruthDerived truth{};
    truth.in_fiducial = false;
    truth.mc_n_strange = 0;
    truth.mc_n_pion = 0;
    truth.mc_n_proton = 0;
    truth.interaction_mode_category = -1;
    truth.inclusive_strange_channel_category = channel;
    truth.exclusive_strange_channel_category = channel;
    truth.channel_definition_category = channel_definition;
    truth.is_truth_signal = false;
    truth.pure_slice_signal = false;
    return truth;
}

ROOT::RDF::RNode defineTruthDerivedColumns(ROOT::RDF::RNode df) {
    const auto truth_column = [](auto member_ptr) {
        return [member_ptr](const TruthDerived &truth) {
            const auto value = truth.*member_ptr;
            return value;
        };
    };

    return df.Define("in_fiducial", truth_column(&TruthDerived::in_fiducial), {"truth_derived"})
        .Define("mc_n_strange", truth_column(&TruthDerived::mc_n_strange), {"truth_derived"})
        .Define("mc_n_pion", truth_column(&TruthDerived::mc_n_pion), {"truth_derived"})
        .Define("mc_n_proton", truth_column(&TruthDerived::mc_n_proton), {"truth_derived"})
        .Define("interaction_mode_category", truth_column(&TruthDerived::interaction_mode_category), {"truth_derived"})
        .Define("inclusive_strange_channel_category",
                truth_column(&TruthDerived::inclusive_strange_channel_category),
                {"truth_derived"})
        .Define("exclusive_strange_channel_category",
                truth_column(&TruthDerived::exclusive_strange_channel_category),
                {"truth_derived"})
        .Define("channel_definition_category",
                truth_column(&TruthDerived::channel_definition_category),
                {"truth_derived"})
        .Define("is_truth_signal", truth_column(&TruthDerived::is_truth_signal), {"truth_derived"})
        .Define("pure_slice_signal", truth_column(&TruthDerived::pure_slice_signal), {"truth_derived"});
}

}

ROOT::RDF::RNode TruthChannelProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    if (st != SampleOrigin::kMonteCarlo) {
        return processData(df, st);
    }

    auto with_truth = df.Define("truth_derived",
                                buildTruthDerived,
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

    return defineTruthDerivedColumns(with_truth);
}

ROOT::RDF::RNode TruthChannelProcessor::processData(ROOT::RDF::RNode df, SampleOrigin st) const {
    const auto [channel, channel_def] = channelInfoForDataSample(st);
    const auto truth_defaults = buildSyntheticTruthDerived(channel, channel_def);

    auto with_truth = df.Define("truth_derived", [truth_defaults]() { return truth_defaults; });

    return defineTruthDerivedColumns(with_truth);
}

}
