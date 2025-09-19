#include <rarexsec/Selections.h>

#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace proc {
namespace selection {
namespace {

bool hasColumnEverywhere(const AnalysisDataLoader::SampleFrameMap &frames, const std::string &column) {
    for (const auto &[sample_key, sample] : frames) {
        (void)sample_key;
        auto node = sample.nominalNode();
        if (!node.HasColumn(column)) {
            return false;
        }
        for (const auto &[variation, variation_node] : sample.variationNodes()) {
            (void)variation;
            auto mutable_node = variation_node;
            if (!mutable_node.HasColumn(column)) {
                return false;
            }
        }
    }
    return true;
}

std::string buildSoftwareTriggerExpression(bool has_run_column, bool has_pre, bool has_post, bool has_pre_ext,
                                           bool has_post_ext, const std::string &is_mc_expr) {
    const std::string software_default = is_mc_expr + " ? (software_trigger > 0) : true";
    if (!has_run_column || !has_pre || !has_post) {
        return software_default;
    }

    std::string pre_expr = "software_trigger_pre > 0";
    if (has_pre_ext) {
        pre_expr = "(" + pre_expr + " || software_trigger_pre_ext > 0)";
    }

    std::string post_expr = "software_trigger_post > 0";
    if (has_post_ext) {
        post_expr = "(" + post_expr + " || software_trigger_post_ext > 0)";
    }

    return is_mc_expr + " ? (run < 16880 ? " + pre_expr + " : " + post_expr + ") : true";
}

} // namespace

SelectionQuery muonNeutrinoCCSelection(const AnalysisDataLoader::SampleFrameMap &frames) {
    if (frames.empty()) {
        return SelectionQuery{};
    }

    const std::vector<std::string> required_columns = {
        "bnbdata",                "extdata",
        "_opfilter_pe_beam",      "_opfilter_pe_veto",     "software_trigger",
        "nslice",                 "topological_score",     "reco_nu_vtx_sce_x",
        "reco_nu_vtx_sce_y",      "reco_nu_vtx_sce_z",     "contained_fraction",
        "slice_cluster_fraction", "has_muon_numu_cc"};

    std::vector<std::string> missing_columns;
    missing_columns.reserve(required_columns.size());

    for (const auto &column : required_columns) {
        if (!hasColumnEverywhere(frames, column)) {
            missing_columns.push_back(column);
        }
    }

    if (!missing_columns.empty()) {
        std::ostringstream message;
        message << "Muon neutrino CC selection requires columns that are not available: ";
        for (std::size_t i = 0; i < missing_columns.size(); ++i) {
            if (i != 0) {
                message << ", ";
            }
            message << missing_columns[i];
        }
        throw std::runtime_error(message.str());
    }

    const bool has_run_column = hasColumnEverywhere(frames, "run");
    const bool has_pre = hasColumnEverywhere(frames, "software_trigger_pre");
    const bool has_post = hasColumnEverywhere(frames, "software_trigger_post");
    const bool has_pre_ext = hasColumnEverywhere(frames, "software_trigger_pre_ext");
    const bool has_post_ext = hasColumnEverywhere(frames, "software_trigger_post_ext");

    const std::string is_mc_expr = "(bnbdata == 0 && extdata == 0)";

    const std::string dataset_gate_expr =
        "(" + is_mc_expr + " ? (_opfilter_pe_beam > 0.f && _opfilter_pe_veto < 20.f) : true)";

    const std::string bnb_trigger_expr = "(bnbdata != 0 ? (software_trigger > 0) : true)";

    const std::string mc_trigger_expr =
        "(" + buildSoftwareTriggerExpression(has_run_column, has_pre, has_post, has_pre_ext, has_post_ext, is_mc_expr) + ")";

    const std::string basic_reco_expr = "(nslice == 1 && topological_score > 0.06f)";

    const std::string vertex_expr =
        "(reco_nu_vtx_sce_x >= 5.f && reco_nu_vtx_sce_x <= 251.f && "
        "reco_nu_vtx_sce_y >= -110.f && reco_nu_vtx_sce_y <= 110.f && "
        "reco_nu_vtx_sce_z >= 20.f && reco_nu_vtx_sce_z <= 986.f && "
        "(reco_nu_vtx_sce_z <= 675.f || reco_nu_vtx_sce_z >= 775.f))";

    const std::string slice_quality_expr = "(contained_fraction >= 0.7f && slice_cluster_fraction >= 0.5f)";

    const std::string muon_expr = "has_muon_numu_cc";

    std::ostringstream selection_stream;
    selection_stream << dataset_gate_expr << " && " << bnb_trigger_expr << " && " << mc_trigger_expr << " && "
                     << basic_reco_expr << " && " << vertex_expr << " && " << slice_quality_expr << " && "
                     << muon_expr;

    return SelectionQuery{selection_stream.str()};
}

} // namespace selection
} // namespace proc
