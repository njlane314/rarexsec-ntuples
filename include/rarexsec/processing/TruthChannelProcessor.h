#ifndef TRUTH_CHANNEL_PROCESSOR_H
#define TRUTH_CHANNEL_PROCESSOR_H

#include <cmath>
#include <iostream>
#include <map>
#include <mutex>

#include <rarexsec/data/IEventProcessor.h>

namespace analysis {

class TruthChannelProcessor : public IEventProcessor {
public:
  explicit TruthChannelProcessor() = default;
  ROOT::RDF::RNode process(ROOT::RDF::RNode df,
                           SampleOrigin st) const override {
    if (st != SampleOrigin::kMonteCarlo) {
      return this->processNonMc(df, st);
    }

    auto counts_df = this->defineCounts(df);

    auto incl_df = this->assignInclusiveChannels(counts_df);

    auto excl_df = this->assignExclusiveChannels(incl_df);

    auto chan_df = this->assignChannelDefinitions(excl_df);

    return next_ ? next_->process(chan_df, st) : chan_df;
  }

private:
  ROOT::RDF::RNode processNonMc(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto mode_df = df.Define("genie_int_mode", []() { return -1; });

    auto incl_df = mode_df.Define("incl_channel", [c = st]() {
      if (c == SampleOrigin::kData)
        return 0; // Data
      if (c == SampleOrigin::kExternal)
        return 1; // External
      if (c == SampleOrigin::kDirt)
        return 2; // Dirt
      return 99;  // Other / Unknown
    });

    auto incl_alias_df =
        incl_df.Define("inclusive_strange_channels", "incl_channel");

    auto excl_df = incl_alias_df.Define("excl_channel", [c = st]() {
      if (c == SampleOrigin::kData)
        return 0; // Data
      if (c == SampleOrigin::kExternal)
        return 1; // External
      if (c == SampleOrigin::kDirt)
        return 2; // Dirt
      return 99;  // Other / Unknown
    });

    auto excl_alias_df =
        excl_df.Define("exclusive_strange_channels", "excl_channel");

    auto chan_df = excl_alias_df.Define("channel_def", [c = st]() {
      if (c == SampleOrigin::kData)
        return 0; // Data
      if (c == SampleOrigin::kExternal || c == SampleOrigin::kDirt)
        return 1; // External
      return 99;  // Other / Unknown
    });

    auto chan_alias_df = chan_df.Define("channel_definitions", "channel_def");

    return next_ ? next_->process(chan_alias_df, st) : chan_alias_df;
  }

  ROOT::RDF::RNode defineCounts(ROOT::RDF::RNode df) const {
    auto fid_df =
        df.Define("in_fiducial",
                  "(neutrino_vertex_x > 5 && neutrino_vertex_x < 251) &&"
                  "(neutrino_vertex_y > -110 && neutrino_vertex_y < 110) &&"
                  "(neutrino_vertex_z > 20 && neutrino_vertex_z < 986)");

    auto strange_df = fid_df.Define(
        "mc_n_strange", "count_kaon_plus + count_kaon_minus + count_kaon_zero +"
                        " count_lambda + count_sigma_plus + count_sigma_zero + "
                        "count_sigma_minus");

    auto pion_df =
        strange_df.Define("mc_n_pion", "count_pi_plus + count_pi_minus");

    auto proton_df = pion_df.Define("mc_n_proton", "count_proton");

    auto mode_df = proton_df.Define(
        "genie_int_mode",
        [](int mode) {
          struct ModeCounter {
            std::map<int, long long> counts;
            std::mutex mtx;
            ~ModeCounter() {
              std::cout << "[DEBUG] GENIE interaction mode frequencies:\n";
              for (const auto &kv : counts) {
                std::cout << "  mode " << kv.first << ": " << kv.second
                          << std::endl;
              }
            }
          };
          static ModeCounter counter;
          {
            std::lock_guard<std::mutex> lock(counter.mtx);
            counter.counts[mode]++;
            if (counter.counts[mode] == 1 && mode != 0 && mode != 1 &&
                mode != 2 && mode != 3 && mode != 10) {
              std::cout << "[DEBUG] Uncategorised GENIE mode: " << mode
                        << std::endl;
            }
          }
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

  ROOT::RDF::RNode assignInclusiveChannels(ROOT::RDF::RNode df) const {
    auto incl_chan_df =
        df.Define("incl_channel",
                  [](bool fv, int nu, int cc, int s, int np, int npi) {
                    if (!fv)
                      return 98; // out_fv
                    if (cc == 1)
                      return 31; // nc
                    if (std::abs(nu) == 12 && cc == 0)
                      return 30; // nue_cc
                    if (std::abs(nu) == 14 && cc == 0) {
                      if (s == 1)
                        return 10; // numu_cc_1s
                      if (s > 1)
                        return 11; // numu_cc_ms
                      if (np >= 1 && npi == 0)
                        return 20; // numu_cc_np0pi
                      if (np == 0 && npi >= 1)
                        return 21; // numu_cc_0pnpi
                      if (np >= 1 && npi >= 1)
                        return 22; // numu_cc_npnpi
                      return 23;   // numu_cc_other
                    }
                    return 99; // other
                  },
                  {"in_fiducial", "neutrino_pdg", "interaction_ccnc",
                   "mc_n_strange", "mc_n_pion", "mc_n_proton"});

    auto incl_alias_df =
        incl_chan_df.Define("inclusive_strange_channels", "incl_channel");

    return incl_alias_df;
  }

  ROOT::RDF::RNode assignExclusiveChannels(ROOT::RDF::RNode df) const {
    auto excl_chan_df = df.Define(
        "excl_channel",
        [](bool fv, int nu, int cc, int s, int kp, int km, int k0, int lam,
           int sp, int s0, int sm) {
          if (!fv)
            return 98; // out_fv
          if (cc == 1)
            return 31; // nc
          if (std::abs(nu) == 12 && cc == 0)
            return 30; // nue_cc
          if (std::abs(nu) == 14 && cc == 0) {
            if (s == 0)
              return 32; // numu_cc_other
            if ((kp == 1 || km == 1) && s == 1)
              return 50; // numu_cc_kpm
            if (k0 == 1 && s == 1)
              return 51; // numu_cc_k0
            if (lam == 1 && s == 1)
              return 52; // numu_cc_lambda
            if ((sp == 1 || sm == 1) && s == 1)
              return 53; // numu_cc_sigmapm
            if (lam == 1 && (kp == 1 || km == 1) && s == 2)
              return 54; // numu_cc_lambda_kpm
            if ((sp == 1 || sm == 1) && k0 == 1 && s == 2)
              return 55; // numu_cc_sigma_k0
            if ((sp == 1 || sm == 1) && (kp == 1 || km == 1) && s == 2)
              return 56; // numu_cc_sigma_kmp
            if (lam == 1 && k0 == 1 && s == 2)
              return 57; // numu_cc_lambda_k0
            if (kp == 1 && km == 1 && s == 2)
              return 58; // numu_cc_kpm_kmp
            if (s0 == 1 && s == 1)
              return 59; // numu_cc_sigma0
            if (s0 == 1 && kp == 1 && s == 2)
              return 60; // numu_cc_sigma0_kpm
            return 61;   // numu_cc_other_strange
          }
          return 99; // other
        },
        {"in_fiducial", "neutrino_pdg", "interaction_ccnc", "mc_n_strange",
         "count_kaon_plus", "count_kaon_minus", "count_kaon_zero",
         "count_lambda", "count_sigma_plus", "count_sigma_zero",
         "count_sigma_minus"});

    auto excl_alias_df =
        excl_chan_df.Define("exclusive_strange_channels", "excl_channel");

    return excl_alias_df;
  }

  ROOT::RDF::RNode assignChannelDefinitions(ROOT::RDF::RNode df) const {
    auto chan_df = df.Define("channel_definitions",
                             [](bool fv, int nu, int cc, int s, int npi, int np,
                                int npi0, int ngamma) {
                               if (!fv) {
                                 if (nu == 0)
                                   return 1; // External
                                 return 2;   // Out FV
                               }
                               if (cc == 1)
                                 return 14; // nc
                               if (cc == 0 && s > 0) {
                                 if (s == 1)
                                   return 15; // numu_cc_1s
                                 return 16;   // numu_cc_ms
                               }
                               if (std::abs(nu) == 12 && cc == 0)
                                 return 17; // nue_cc
                               if (std::abs(nu) == 14 && cc == 0) {
                                 if (npi == 0 && np > 0)
                                   return 10; // numu_cc_np0pi
                                 if (npi == 1 && npi0 == 0)
                                   return 11; // numu_cc_0pnpi
                                 if (npi0 > 0 || ngamma >= 2)
                                   return 12; // numu_cc_pi0gg
                                 if (npi > 1)
                                   return 13; // numu_cc_npnpi
                                 return 18;   // numu_cc_other
                               }
                               return 99; // other
                             },
                             {"in_fiducial", "neutrino_pdg", "interaction_ccnc",
                              "mc_n_strange", "mc_n_pion", "mc_n_proton",
                              "count_pi_zero", "count_gamma"});

    auto signal_df = chan_def.Define("is_truth_signal",
                             [](int ch) { return ch == 15 || ch == 16; },
                             {"channel_definitions"});

    auto pure_sig_df = signal_df.Define(
        "pure_slice_signal",
        [](bool is_sig, float purity, float completeness) {
          return is_sig && purity > 0.5f && completeness > 0.1f;
        },
        {"is_truth_signal", "neutrino_purity_from_pfp",
         "neutrino_completeness_from_pfp"});

    return pure_sig_df;
  }
};

} 

#endif
