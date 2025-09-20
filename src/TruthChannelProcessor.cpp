#include <rarexsec/TruthChannelProcessor.h>
#include <rarexsec/Catalogue.h>

#include <cmath>

namespace proc {
namespace {

int channelForNonMcSample(SampleOrigin origin) {
    switch (origin) {
    case SampleOrigin::kData:
        return 0;
    case SampleOrigin::kExternal:
        return 1;
    case SampleOrigin::kDirt:
        return 2;
    default:
        return 99;
    }
}

int channelDefinitionForNonMcSample(SampleOrigin origin) {
    switch (origin) {
    case SampleOrigin::kData:
        return 0;
    case SampleOrigin::kExternal:
    case SampleOrigin::kDirt:
        return 1;
    default:
        return 99;
    }
}

}

ROOT::RDF::RNode TruthChannelProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    if (st != SampleOrigin::kMonteCarlo) {
        return processNonMc(df, st);
    }

    auto counts_df = defineCounts(df);
    auto incl_df = assignInclusiveChannels(counts_df);
    auto excl_df = assignExclusiveChannels(incl_df);
    auto chan_df = assignChannelDefinitions(excl_df);
    return next_ ? next_->process(chan_df, st) : chan_df;
}

ROOT::RDF::RNode TruthChannelProcessor::processNonMc(ROOT::RDF::RNode df, SampleOrigin st) const {
    const auto channel = channelForNonMcSample(st);
    const auto channel_def = channelDefinitionForNonMcSample(st);

    auto mode_df = df.Define("genie_int_mode", []() { return -1; });

    auto channels_df =
        mode_df.Define("incl_channel", [channel]() { return channel; })
            .Alias("inclusive_strange_channels", "incl_channel")
            .Define("excl_channel", [channel]() { return channel; })
            .Alias("exclusive_strange_channels", "excl_channel")
            .Define("channel_def", [channel_def]() { return channel_def; })
            .Alias("channel_definitions", "channel_def");

    return next_ ? next_->process(channels_df, st) : channels_df;
}

ROOT::RDF::RNode TruthChannelProcessor::defineCounts(ROOT::RDF::RNode df) const {
    auto fid_df = df.Define(
        "in_fiducial",
        [](float x, float y, float z) { return ::proc::isInFiducialVolume(x, y, z); },
        {"neutrino_vertex_x", "neutrino_vertex_y", "neutrino_vertex_z"});

    auto strange_df = fid_df.Define(
        "mc_n_strange",
        "count_kaon_plus + count_kaon_minus + count_kaon_zero + count_lambda + count_sigma_plus + count_sigma_zero + count_sigma_minus");

    auto pion_df = strange_df.Define("mc_n_pion", "count_pi_plus + count_pi_minus");

    auto proton_df = pion_df.Define("mc_n_proton", "count_proton");

    auto mode_df = proton_df.Define(
        "genie_int_mode",
        [](int mode) {
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
        },
        {"interaction_mode"});

    return mode_df;
}

ROOT::RDF::RNode TruthChannelProcessor::assignInclusiveChannels(ROOT::RDF::RNode df) const {
    auto incl_chan_df = df.Define(
        "incl_channel",
        [](bool fv, int nu, int cc, int s, int np, int npi) {
            if (!fv) {
                return 98;
            }
            if (cc == 1) {
                return 31;
            }
            if (std::abs(nu) == 12 && cc == 0) {
                return 30;
            }
            if (std::abs(nu) == 14 && cc == 0) {
                if (s == 1) {
                    return 10;
                }
                if (s > 1) {
                    return 11;
                }
                if (np >= 1 && npi == 0) {
                    return 20;
                }
                if (np == 0 && npi >= 1) {
                    return 21;
                }
                if (np >= 1 && npi >= 1) {
                    return 22;
                }
                return 23;
            }
            return 99;
        },
        {"in_fiducial", "neutrino_pdg", "interaction_ccnc", "mc_n_strange", "mc_n_proton", "mc_n_pion"});

    auto incl_alias_df = incl_chan_df.Define("inclusive_strange_channels", "incl_channel");

    return incl_alias_df;
}

ROOT::RDF::RNode TruthChannelProcessor::assignExclusiveChannels(ROOT::RDF::RNode df) const {
    auto excl_chan_df = df.Define(
        "excl_channel",
        [](bool fv, int nu, int cc, int s, int kp, int km, int k0, int lam, int sp, int s0, int sm) {
            if (!fv) {
                return 98;
            }
            if (cc == 1) {
                return 31;
            }
            if (std::abs(nu) == 12 && cc == 0) {
                return 30;
            }
            if (std::abs(nu) == 14 && cc == 0) {
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
                if (lam == 1 && (kp == 1 || km == 1) && s == 2) {
                    return 54;
                }
                if ((sp == 1 || sm == 1) && k0 == 1 && s == 2) {
                    return 55;
                }
                if ((sp == 1 || sm == 1) && (kp == 1 || km == 1) && s == 2) {
                    return 56;
                }
                if (lam == 1 && k0 == 1 && s == 2) {
                    return 57;
                }
                if (kp == 1 && km == 1 && s == 2) {
                    return 58;
                }
                if (s0 == 1 && s == 1) {
                    return 59;
                }
                if (s0 == 1 && kp == 1 && s == 2) {
                    return 60;
                }
                return 61;
            }
            return 99;
        },
        {"in_fiducial", "neutrino_pdg", "interaction_ccnc", "mc_n_strange", "count_kaon_plus", "count_kaon_minus",
         "count_kaon_zero", "count_lambda", "count_sigma_plus", "count_sigma_zero", "count_sigma_minus"});

    auto excl_alias_df = excl_chan_df.Define("exclusive_strange_channels", "excl_channel");

    return excl_alias_df;
}

ROOT::RDF::RNode TruthChannelProcessor::assignChannelDefinitions(ROOT::RDF::RNode df) const {
    auto chan_df = df.Define(
        "channel_def",
        [](bool fv, int nu, int cc, int s, int npi, int np, int npi0, int ngamma) {
            if (!fv) {
                if (nu == 0) {
                    return 1;
                }
                return 2;
            }
            if (cc == 1) {
                return 14;
            }
            if (cc == 0 && s > 0) {
                if (s == 1) {
                    return 15;
                }
                return 16;
            }
            if (std::abs(nu) == 12 && cc == 0) {
                return 17;
            }
            if (std::abs(nu) == 14 && cc == 0) {
                if (npi == 0 && np > 0) {
                    return 10;
                }
                if (npi == 1 && npi0 == 0) {
                    return 11;
                }
                if (npi0 > 0 || ngamma >= 2) {
                    return 12;
                }
                if (npi > 1) {
                    return 13;
                }
                return 18;
            }
            return 99;
        },
        {"in_fiducial", "neutrino_pdg", "interaction_ccnc", "mc_n_strange", "mc_n_pion", "mc_n_proton",
         "count_pi_zero", "count_gamma"});

    auto alias_df = chan_df.Define("channel_definitions", "channel_def");

    auto signal_df = alias_df.Define("is_truth_signal", [](int ch) { return ch == 15 || ch == 16; },
                                     {"channel_definitions"});

    auto pure_sig_df = signal_df.Define(
        "pure_slice_signal",
        [](bool is_sig, float purity, float completeness) {
            return is_sig && purity > 0.5f && completeness > 0.1f;
        },
        {"is_truth_signal", "neutrino_purity_from_pfp", "neutrino_completeness_from_pfp"});

    return pure_sig_df;
}

}
