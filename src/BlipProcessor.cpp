#include <rarexsec/BlipProcessor.h>

#include <cmath>
#include <cstddef>
#include <functional>
#include <string_view>
#include <unordered_map>

namespace {

int encodeBlipProcess(const std::string &process) {
    static const std::unordered_map<std::string_view, int, std::hash<std::string_view>> kProcessCodes{
        {"", 0},
        {"null", 0},
        {"muMinusCaptureAtRest", 1},
        {"nCapture", 2},
        {"neutronInelastic", 3},
        {"compt", 4},
        {"phot", 4},
        {"conv", 4},
        {"eIoni", 5},
        {"eBrem", 5},
        {"muIoni", 6},
        {"hIoni", 7},
    };

    const auto match = kProcessCodes.find(process);
    return match != kProcessCodes.end() ? match->second : -1;
}

ROOT::RVec<int> encodeBlipProcesses(const ROOT::RVec<std::string> &processes) {
    ROOT::RVec<int> codes;
    codes.reserve(processes.size());
    for (const auto &process : processes) {
        codes.emplace_back(encodeBlipProcess(process));
    }
    return codes;
}

float distanceToVertex(float x, float y, float z, float vx, float vy, float vz) {
    const float dx = x - vx;
    const float dy = y - vy;
    const float dz = z - vz;
    return std::sqrt(dx * dx + dy * dy + dz * dz);
}

ROOT::RVec<float> computeDistancesToVertex(const ROOT::RVec<float> &bx, const ROOT::RVec<float> &by,
                                           const ROOT::RVec<float> &bz, float vx, float vy, float vz) {
    ROOT::RVec<float> distances;
    distances.reserve(bx.size());
    for (std::size_t i = 0; i < bx.size(); ++i) {
        distances.emplace_back(distanceToVertex(bx[i], by[i], bz[i], vx, vy, vz));
    }
    return distances;
}

ROOT::RVec<float> missingVertexDistances(const ROOT::RVec<float> &bx) {
    return ROOT::RVec<float>(bx.size(), -1.f);
}

}

namespace proc {

ROOT::RDF::RNode BlipProcessor::process(ROOT::RDF::RNode df, SampleOrigin st) const {
    auto proc_df = df.Define("blip_process_code", encodeBlipProcesses, {"blip_process"});

    if (proc_df.HasColumn("neutrino_vertex_x")) {
        proc_df = proc_df.Define("blip_distance_to_vertex", computeDistancesToVertex,
                                 {"blip_x",
                                  "blip_y",
                                  "blip_z",
                                  "neutrino_vertex_x",
                                  "neutrino_vertex_y",
                                  "neutrino_vertex_z"});
    } else {
        proc_df = proc_df.Define("blip_distance_to_vertex", missingVertexDistances, {"blip_x"});
    }

    return proc_df;
}

}
