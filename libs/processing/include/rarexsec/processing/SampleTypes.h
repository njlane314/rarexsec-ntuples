#ifndef SAMPLE_TYPES_H
#define SAMPLE_TYPES_H

#include <string>
#include <string_view>

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

inline SampleOrigin originFromString(std::string_view type) {
    if (type == "mc") {
        return SampleOrigin::kMonteCarlo;
    }
    if (type == "data") {
        return SampleOrigin::kData;
    }
    if (type == "ext") {
        return SampleOrigin::kExternal;
    }
    if (type == "dirt") {
        return SampleOrigin::kDirt;
    }
    return SampleOrigin::kUnknown;
}

inline std::string originToString(SampleOrigin origin) {
    switch (origin) {
    case SampleOrigin::kData:
        return "data";
    case SampleOrigin::kMonteCarlo:
        return "mc";
    case SampleOrigin::kExternal:
        return "ext";
    case SampleOrigin::kDirt:
        return "dirt";
    default:
        return "unknown";
    }
}

inline SampleVariation variationFromString(std::string_view variation) {
    if (variation == "cv") {
        return SampleVariation::kCV;
    }
    if (variation == "lyatt") {
        return SampleVariation::kLYAttenuation;
    }
    if (variation == "lydown") {
        return SampleVariation::kLYDown;
    }
    if (variation == "lyray") {
        return SampleVariation::kLYRayleigh;
    }
    if (variation == "recomb2") {
        return SampleVariation::kRecomb2;
    }
    if (variation == "sce") {
        return SampleVariation::kSCE;
    }
    if (variation == "wiremodx") {
        return SampleVariation::kWireModX;
    }
    if (variation == "wiremodyz") {
        return SampleVariation::kWireModYZ;
    }
    if (variation == "wiremodanglexz") {
        return SampleVariation::kWireModAngleXZ;
    }
    if (variation == "wiremodangleyz") {
        return SampleVariation::kWireModAngleYZ;
    }
    return SampleVariation::kUnknown;
}

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
