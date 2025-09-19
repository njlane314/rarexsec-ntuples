#ifndef BLIP_PROCESSOR_H
#define BLIP_PROCESSOR_H

#include <rarexsec/data/IEventProcessor.h>
#include "ROOT/RVec.hxx"
#include <cmath>
#include <string>

namespace analysis {

class BlipProcessor : public IEventProcessor {
  public:
    ROOT::RDF::RNode process(ROOT::RDF::RNode df, SampleOrigin st) const override {
        auto proc_df = df.Define("blip_process_code",
                                 [](const ROOT::RVec<std::string> &processes) {
                                     ROOT::RVec<int> codes(processes.size());
                                     for (size_t i = 0; i < processes.size(); ++i) {
                                         const auto &p = processes[i];
                                         int code = -1;
                                         if (p == "null" || p.empty())
                                             code = 0;
                                         else if (p == "muMinusCaptureAtRest")
                                             code = 1;
                                         else if (p == "nCapture")
                                             code = 2;
                                         else if (p == "neutronInelastic")
                                             code = 3;
                                         else if (p == "compt" || p == "phot" || p == "conv")
                                             code = 4;
                                         else if (p == "eIoni" || p == "eBrem")
                                             code = 5;
                                         else if (p == "muIoni")
                                             code = 6;
                                         else if (p == "hIoni")
                                             code = 7;
                                         codes[i] = code;
                                     }
                                     return codes;
                                 },
                                 {"blip_process"});

        if (proc_df.HasColumn("neutrino_vertex_x")) {
            proc_df = proc_df.Define(
                "blip_distance_to_vertex",
                [](const ROOT::RVec<float> &bx, const ROOT::RVec<float> &by, const ROOT::RVec<float> &bz, float vx,
                   float vy, float vz) {
                    ROOT::RVec<float> dists(bx.size());
                    for (size_t i = 0; i < bx.size(); ++i) {
                        float dx = bx[i] - vx;
                        float dy = by[i] - vy;
                        float dz = bz[i] - vz;
                        dists[i] = std::sqrt(dx * dx + dy * dy + dz * dz);
                    }
                    return dists;
                },
                {"blip_x", "blip_y", "blip_z", "neutrino_vertex_x", "neutrino_vertex_y", "neutrino_vertex_z"});
        } else {
            proc_df = proc_df.Define("blip_distance_to_vertex",
                                     [](const ROOT::RVec<float> &bx) { return ROOT::RVec<float>(bx.size(), -1.f); },
                                     {"blip_x"});
        }

        return next_ ? next_->process(proc_df, st) : proc_df;
    }
};

}

#endif
