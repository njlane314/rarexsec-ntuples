#ifndef SAMPLE_TYPES_H
#define SAMPLE_TYPES_H

#include <string>

namespace proc {

enum class SampleOrigin : unsigned int { kUnknown = 0, kData, kMonteCarlo, kExternal, kDirt };

enum class AnalysisRole { kData, kNominal, kVariation };

enum class SampleVariation : unsigned int {
    kUnknown = 0,
    kCV,
    kLYAttenuation,
    kLYDown,
    kLYRayleigh,
    kRecomb2,
    kSCE,
    kWireModX,
    kWireModYZ,
    kWireModAngleXZ,
    kWireModAngleYZ
};

inline std::string variationToKey(SampleVariation var) {
    switch (var) {
    case SampleVariation::kCV:
        return "CV";
    case SampleVariation::kLYAttenuation:
        return "LYAttenuation";
    case SampleVariation::kLYDown:
        return "LYDown";
    case SampleVariation::kLYRayleigh:
        return "LYRayleigh";
    case SampleVariation::kRecomb2:
        return "Recomb2";
    case SampleVariation::kSCE:
        return "SCE";
    case SampleVariation::kWireModX:
        return "WireModX";
    case SampleVariation::kWireModYZ:
        return "WireModYZ";
    case SampleVariation::kWireModAngleXZ:
        return "WireModAngleXZ";
    case SampleVariation::kWireModAngleYZ:
        return "WireModAngleYZ";
    default:
        return "Unknown";
    }
}

}

#endif
