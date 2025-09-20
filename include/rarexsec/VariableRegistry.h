#ifndef EVENT_VARIABLE_REGISTRY_H
#define EVENT_VARIABLE_REGISTRY_H

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <rarexsec/SampleTypes.h>

namespace proc {

class VariableRegistry {
  public:
    using KnobVariations =
        std::unordered_map<std::string, std::pair<std::string, std::string>>;
    using MultiUniverseVars = std::unordered_map<std::string, unsigned>;
    using ColumnCollection = std::vector<std::string>;

    struct ColumnPlan {
        ColumnCollection required;
        ColumnCollection optional;
    };

    void includeCommonColumn(const std::string &column) { common_required_columns_.push_back(column); }

    void includeCommonColumns(const ColumnCollection &columns) {
        common_required_columns_.insert(common_required_columns_.end(), columns.begin(), columns.end());
    }

    void includeCommonOptionalColumn(const std::string &column) { common_optional_columns_.push_back(column); }

    void includeCommonOptionalColumns(const ColumnCollection &columns) {
        common_optional_columns_.insert(common_optional_columns_.end(), columns.begin(), columns.end());
    }

    void includeRequiredColumn(SampleOrigin origin, const std::string &column) {
        origin_column_plans_[origin].required.push_back(column);
    }

    void includeRequiredColumns(SampleOrigin origin, const ColumnCollection &columns) {
        auto &bucket = origin_column_plans_[origin].required;
        bucket.insert(bucket.end(), columns.begin(), columns.end());
    }

    void includeOptionalColumn(SampleOrigin origin, const std::string &column) {
        origin_column_plans_[origin].optional.push_back(column);
    }

    void includeOptionalColumns(SampleOrigin origin, const ColumnCollection &columns) {
        auto &bucket = origin_column_plans_[origin].optional;
        bucket.insert(bucket.end(), columns.begin(), columns.end());
    }

    void includeColumn(SampleOrigin origin, const std::string &column) { includeRequiredColumn(origin, column); }

    void includeColumns(SampleOrigin origin, const ColumnCollection &columns) {
        includeRequiredColumns(origin, columns);
    }

    ColumnPlan columnPlanFor(SampleOrigin type) const {
        ColumnPlan plan;
        std::unordered_set<std::string> required_seen;
        for (const auto &column : eventVariables(type)) {
            if (required_seen.insert(column).second) {
                plan.required.push_back(column);
            }
        }

        for (const auto &column : common_required_columns_) {
            if (required_seen.insert(column).second) {
                plan.required.push_back(column);
            }
        }

        auto origin_it = origin_column_plans_.find(type);
        if (origin_it != origin_column_plans_.end()) {
            for (const auto &column : origin_it->second.required) {
                if (required_seen.insert(column).second) {
                    plan.required.push_back(column);
                }
            }
        }

        std::unordered_set<std::string> optional_seen;
        for (const auto &column : common_optional_columns_) {
            if (!required_seen.count(column) && optional_seen.insert(column).second) {
                plan.optional.push_back(column);
            }
        }

        if (origin_it != origin_column_plans_.end()) {
            for (const auto &column : origin_it->second.optional) {
                if (!required_seen.count(column) && optional_seen.insert(column).second) {
                    plan.optional.push_back(column);
                }
            }
        }

        return plan;
    }

    ColumnCollection columnsFor(SampleOrigin type) const {
        ColumnCollection columns;
        const auto plan = columnPlanFor(type);
        columns.reserve(plan.required.size() + plan.optional.size());
        columns.insert(columns.end(), plan.required.begin(), plan.required.end());
        columns.insert(columns.end(), plan.optional.begin(), plan.optional.end());
        return columns;
    }

    static const KnobVariations &knobVariations() {
        static const KnobVariations m = {{"RPA", {"knobRPAup", "knobRPAdn"}},
                                         {"CCMEC", {"knobCCMECup", "knobCCMECdn"}},
                                         {"AxFFCCQE", {"knobAxFFCCQEup", "knobAxFFCCQEdn"}},
                                         {"VecFFCCQE", {"knobVecFFCCQEup", "knobVecFFCCQEdn"}},
                                         {"DecayAngMEC", {"knobDecayAngMECup", "knobDecayAngMECdn"}},
                                         {"ThetaDelta2Npi", {"knobThetaDelta2Npiup", "knobThetaDelta2Npidn"}},
                                         {"ThetaDelta2NRad", {"knobThetaDelta2NRadup", "knobThetaDelta2NRaddn"}},
                                         {"NormCCCOH", {"knobNormCCCOHup", "knobNormCCCOHdn"}},
                                         {"NormNCCOH", {"knobNormNCCOHup", "knobNormNCCOHdn"}},
                                         {"xsr_scc_Fv3", {"knobxsr_scc_Fv3up", "knobxsr_scc_Fv3dn"}},
                                         {"xsr_scc_Fa3", {"knobxsr_scc_Fa3up", "knobxsr_scc_Fa3dn"}}};

        return m;
    }

    static const MultiUniverseVars &multiUniverseVariations() {
        static const MultiUniverseVars m = {
            {"weightsGenie", 500},
            {"weightsFlux", 500},
            {"weightsReint", 500},
            {"weightsPPFX", 500}};

        return m;
    }

    static const std::string &singleKnobVar() {
        static const std::string s = "RootinoFix";

        return s;
    }

    static std::vector<std::string> eventVariables(SampleOrigin type) {
        auto vars = collectBaseGroups();

        if (type == SampleOrigin::kMonteCarlo || type == SampleOrigin::kDirt) {
            appendMonteCarloGroups(vars);
        }

        return std::vector<std::string>(vars.begin(), vars.end());
    }

  private:
    static std::unordered_set<std::string> collectBaseGroups() {
        std::unordered_set<std::string> vars{baseVariables().begin(), baseVariables().end()};

        vars.insert(recoEventVariables().begin(), recoEventVariables().end());
        vars.insert(recoTrackVariables().begin(), recoTrackVariables().end());
        vars.insert(processedEventVariables().begin(), processedEventVariables().end());
        vars.insert(blipVariables().begin(), blipVariables().end());
        vars.insert(imageVariables().begin(), imageVariables().end());
        vars.insert(flashVariables().begin(), flashVariables().end());
        vars.insert(energyVariables().begin(), energyVariables().end());
        vars.insert(sliceVariables().begin(), sliceVariables().end());

        return vars;
    }

    static void appendMonteCarloGroups(std::unordered_set<std::string> &vars) {
        vars.insert(truthVariables().begin(), truthVariables().end());

        for (auto &kv : knobVariations()) {
            vars.insert(kv.second.first);
            vars.insert(kv.second.second);
        }

        for (auto &kv : multiUniverseVariations()) {
            vars.insert(kv.first);
        }

        vars.insert(singleKnobVar());
        vars.insert("weightSpline");
        vars.insert("weightTune");
    }

    static const std::vector<std::string> &baseVariables() {
        static const std::vector<std::string> v = {
            "run",
            "sub",
            "evt"};

        return v;
    }

  static const std::vector<std::string> &truthVariables() {
    static const std::vector<std::string> v = {
        "neutrino_pdg",
        "interaction_ccnc",
        "interaction_mode",
        "interaction_type",
        "neutrino_energy",
        "neutrino_theta",
        "neutrino_pt",
        "target_nucleus_pdg",
        "hit_nucleon_pdg",
        "kinematic_W",
        "kinematic_X",
        "kinematic_Y",
        "kinematic_Q_squared",
        "neutrino_momentum_x",
        "neutrino_momentum_y",
        "neutrino_momentum_z",
        "neutrino_vertex_x",
        "neutrino_vertex_y",
        "neutrino_vertex_z",
        "neutrino_vertex_wire_u",
        "neutrino_vertex_wire_v",
        "neutrino_vertex_wire_w",
        "neutrino_vertex_time",
        "neutrino_sce_vertex_x",
        "neutrino_sce_vertex_y",
        "neutrino_sce_vertex_z",
        "lepton_energy",
        "true_neutrino_momentum_x",
        "true_neutrino_momentum_y",
        "true_neutrino_momentum_z",
        "flux_path_length",
        "flux_parent_pdg",
        "flux_hadron_pdg",
        "flux_decay_mode",
        "flux_decay_vtx_x",
        "flux_decay_vtx_y",
        "flux_decay_vtx_z",
        "flux_decay_mom_x",
        "flux_decay_mom_y",
        "flux_decay_mom_z",
        "numi_baseline",
        "numi_off_axis_angle",
        "bnb_baseline",
        "bnb_off_axis_angle",
        "is_vertex_in_fiducial",
        "count_mu_minus",
        "count_mu_plus",
        "count_e_minus",
        "count_e_plus",
        "count_pi_zero",
        "count_pi_plus",
        "count_pi_minus",
        "count_kaon_plus",
        "count_kaon_minus",
        "count_kaon_zero",
        "count_proton",
        "count_neutron",
        "count_gamma",
        "count_lambda",
        "count_sigma_plus",
        "count_sigma_zero",
        "count_sigma_minus",
        "mc_particle_pdg",
        "mc_particle_trackid",
        "mc_particle_energy",
        "mc_elastic_scatters",
        "mc_inelastic_scatters",
        "mc_momentum_x",
        "mc_momentum_y",
        "mc_momentum_z",
        "mc_end_momentum",
        "mc_start_vertex_x",
        "mc_start_vertex_y",
        "mc_start_vertex_z",
        "mc_end_vertex_x",
        "mc_end_vertex_y",
        "mc_end_vertex_z",
        "mc_particle_final_state",
        "mc_completeness",
        "mc_purity",
        "mc_daughter_pdg",
        "mc_daughter_energy",
        "mc_daughter_process_flat",
        "mc_daughter_process_idx",
        "mc_daughter_mom_x",
        "mc_daughter_mom_y",
        "mc_daughter_mom_z",
        "mc_daughter_vtx_x",
        "mc_daughter_vtx_y",
        "mc_daughter_vtx_z",
        "mc_allchain_primary_index",
        "mc_allchain_trackid",
        "mc_allchain_pdg",
        "mc_allchain_energy",
        "mc_allchain_elastic_scatters",
        "mc_allchain_inelastic_scatters",
        "mc_allchain_momentum_x",
        "mc_allchain_momentum_y",
        "mc_allchain_momentum_z",
        "mc_allchain_end_momentum",
        "mc_allchain_start_vertex_x",
        "mc_allchain_start_vertex_y",
        "mc_allchain_start_vertex_z",
        "mc_allchain_end_vertex_x",
        "mc_allchain_end_vertex_y",
        "mc_allchain_end_vertex_z",
        "mc_allchain_parent_trackid",
        "mc_allchain_process",
        "mc_allchain_final_state",
        "mc_allchain_completeness",
        "mc_allchain_purity",
        "true_transverse_momentum",
        "true_visible_transverse_momentum",
        "true_total_momentum",
        "true_visible_total_momentum",
        "true_visible_energy",
        "neutrino_completeness_from_pfp",
        "neutrino_purity_from_pfp",
        "backtracked_pdg_codes",
        "blip_pdg"};

    return v;
  }

  static const std::vector<std::string> &recoEventVariables() {
    static const std::vector<std::string> v = {"reco_neutrino_vertex_sce_x",
                                               "reco_neutrino_vertex_sce_y",
                                               "reco_neutrino_vertex_sce_z",
                                               "num_slices",
                                               "slice_num_hits",
                                               "selection_pass",
                                               "slice_id",
                                               "optical_filter_pe_beam",
                                               "optical_filter_pe_veto",
                                               "num_pfps",
                                               "num_tracks",
                                               "num_showers",
                                               "event_total_hits",
                                               "crt_veto",
                                               "crt_hit_pe",
                                               "pfp_slice_indices",
                                               "backtracked_pdg_codes",
                                               "backtracked_energies",
                                               "backtracked_track_ids",
                                               "backtracked_purities",
                                               "backtracked_completenesses",
                                               "backtracked_overlay_purities",
                                               "backtracked_momentum_x",
                                               "backtracked_momentum_y",
                                               "backtracked_momentum_z",
                                               "backtracked_start_x",
                                               "backtracked_start_y",
                                               "backtracked_start_z",
                                               "backtracked_start_time",
                                               "backtracked_start_wire_U",
                                               "backtracked_start_wire_V",
                                               "backtracked_start_wire_Y",
                                               "backtracked_sce_start_x",
                                               "backtracked_sce_start_y",
                                               "backtracked_sce_start_z",
                                               "backtracked_sce_start_wire_U",
                                               "backtracked_sce_start_wire_V",
                                               "backtracked_sce_start_wire_Y",
                                               "software_trigger",
                                               "software_trigger_pre",
                                               "software_trigger_post",
                                               "software_trigger_pre_ext",
                                               "software_trigger_post_ext",
                                               "slice_pdg",
                                               "pfp_generations",
                                               "pfp_track_daughters",
                                               "pfp_shower_daughters",
                                               "pfp_num_descendents",
                                               "pfp_vertex_x",
                                               "pfp_vertex_y",
                                               "pfp_vertex_z",
                                               "track_shower_scores",
                                               "pfp_pdg_codes",
                                               "pfp_num_hits",
                                               "pfp_num_plane_hits_U",
                                               "pfp_num_plane_hits_V",
                                               "pfp_num_plane_hits_Y",
                                               "pfp_num_subclusters_U",
                                               "pfp_num_subclusters_V",
                                               "pfp_num_subclusters_Y",
                                               "pfp_max_subhit_fraction_U",
                                               "pfp_max_subhit_fraction_V",
                                               "pfp_max_subhit_fraction_Y",
                                               "total_hits_U",
                                               "total_hits_V",
                                               "total_hits_Y",
                                               "slice_topological_scores",
                                               "topological_score",
                                               "slice_cluster_fraction",
                                               "contained_fraction"};

    return v;
  }

  static const std::vector<std::string> &blipVariables() {
    static const std::vector<std::string> v = {"blip_id",
                                               "blip_is_valid",
                                               "blip_tpc",
                                               "blip_n_planes",
                                               "blip_max_wire_span",
                                               "blip_energy",
                                               "blip_energy_estar",
                                               "blip_time",
                                               "blip_prox_trk_dist",
                                               "blip_prox_trk_id",
                                               "blip_in_cylinder",
                                               "blip_x",
                                               "blip_y",
                                               "blip_z",
                                               "blip_sigma_yz",
                                               "blip_dx",
                                               "blip_dyz",
                                               "blip_charge",
                                               "blip_lead_g4_id",
                                               "blip_pdg",
                                               "blip_process",
                                               "blip_process_code",
                                               "blip_vx",
                                               "blip_vy",
                                               "blip_vz",
                                               "blip_e",
                                               "blip_mass",
                                               "blip_trk_id",
                                               "blip_distance_to_vertex"};

    return v;
  }

  static const std::vector<std::string> &imageVariables() {
    static const std::vector<std::string> v = {"reco_neutrino_vertex_x",
                                               "reco_neutrino_vertex_y",
                                               "reco_neutrino_vertex_z",
                                               "detector_image_u",
                                               "detector_image_v",
                                               "detector_image_w",
                                               "semantic_image_u",
                                               "semantic_image_v",
                                               "semantic_image_w",
                                               "event_detector_image_u",
                                               "event_detector_image_v",
                                               "event_detector_image_w",
                                               "event_semantic_image_u",
                                               "event_semantic_image_v",
                                               "event_semantic_image_w",
                                               "event_adc_u",
                                               "event_adc_v",
                                               "event_adc_w",
                                               "slice_semantic_counts_u",
                                               "slice_semantic_counts_v",
                                               "slice_semantic_counts_w",
                                               "event_semantic_counts_u",
                                               "event_semantic_counts_v",
                                               "event_semantic_counts_w",
                                               "is_vtx_in_image_u",
                                               "is_vtx_in_image_v",
                                               "is_vtx_in_image_w",
                                               "inference_score"};

    return v;
  }

  static const std::vector<std::string> &flashVariables() {
    static const std::vector<std::string> v = {"t0",
                                               "flash_match_score",
                                               "flash_total_pe",
                                               "flash_time",
                                               "flash_z_centre",
                                               "flash_z_width",
                                               "slice_charge",
                                               "slice_z_centre",
                                               "charge_light_ratio",
                                               "flash_slice_z_dist",
                                               "flash_pe_per_charge"};

    return v;
  }

  static const std::vector<std::string> &energyVariables() {
    static const std::vector<std::string> v = {
        "neutrino_energy_0",   "neutrino_energy_1",   "neutrino_energy_2",
        "slice_calo_energy_0", "slice_calo_energy_1", "slice_calo_energy_2"};

    return v;
  }

  static const std::vector<std::string> &sliceVariables() {
    static const std::vector<std::string> v = {"original_event_neutrino_hits",
                                               "event_neutrino_hits",
                                               "event_muon_hits",
                                               "event_electron_hits",
                                               "event_proton_hits",
                                               "event_charged_pion_hits",
                                               "event_neutral_pion_hits",
                                               "event_neutron_hits",
                                               "event_gamma_hits",
                                               "event_other_hits",
                                               "event_charged_kaon_hits",
                                               "event_neutral_kaon_hits",
                                               "event_lambda_hits",
                                               "event_charged_sigma_hits",
                                               "event_sigma_zero_hits",
                                               "event_cosmic_hits",
                                               "slice_neutrino_hits",
                                               "slice_muon_hits",
                                               "slice_electron_hits",
                                               "slice_proton_hits",
                                               "slice_charged_pion_hits",
                                               "slice_neutral_pion_hits",
                                               "slice_neutron_hits",
                                               "slice_gamma_hits",
                                               "slice_other_hits",
                                               "slice_charged_kaon_hits",
                                               "slice_neutral_kaon_hits",
                                               "slice_lambda_hits",
                                               "slice_charged_sigma_hits",
                                               "slice_sigma_zero_hits",
                                               "slice_cosmic_hits",
                                               "pfp_neutrino_hits",
                                               "pfp_muon_hits",
                                               "pfp_electron_hits",
                                               "pfp_proton_hits",
                                               "pfp_charged_pion_hits",
                                               "pfp_neutral_pion_hits",
                                               "pfp_neutron_hits",
                                               "pfp_gamma_hits",
                                               "pfp_other_hits",
                                               "pfp_charged_kaon_hits",
                                               "pfp_neutral_kaon_hits",
                                               "pfp_lambda_hits",
                                               "pfp_charged_sigma_hits",
                                               "pfp_sigma_zero_hits",
                                               "pfp_cosmic_hits",
                                               "neutrino_completeness_from_pfp",
                                               "neutrino_purity_from_pfp"};

    return v;
  }

  static const std::vector<std::string> &recoTrackVariables() {
    static const std::vector<std::string> v = {
        "track_shower_scores",     
        "trk_llr_pid_v",
        "track_length",            
        "track_distance_to_vertex",
        "track_start_x",       
        "track_start_y",
        "track_start_z",       
        "track_end_x",
        "track_end_y",         
        "track_end_z",
        "track_theta",         
        "track_phi",
        "track_calo_energy_u", 
        "track_calo_energy_v",
        "track_calo_energy_y"};

    return v;
  }

    static const std::vector<std::string> &processedEventVariables() {
    static const std::vector<std::string> v = {"in_reco_fiducial",
                                               "reco_neutrino_vertex_sce_x",
                                               "reco_neutrino_vertex_sce_y",
                                               "reco_neutrino_vertex_sce_z",
                                               "n_pfps_gen2",
                                               "n_pfps_gen3",
                                               "quality_event",
                                               "n_muons_tot",
                                               "has_muon",
                                               "muon_trk_score_v",
                                               "muon_trk_llr_pid_v",
                                               "muon_trk_start_x_v",
                                               "muon_trk_start_y_v",
                                               "muon_trk_start_z_v",
                                               "muon_trk_end_x_v",
                                               "muon_trk_end_y_v",
                                               "muon_trk_end_z_v",
                                               "muon_trk_length_v",
                                               "muon_trk_distance_v",
                                               "muon_pfp_generation_v",
                                               "muon_trk_range_muon_mom_v",
                                               "muon_track_costheta",
                                               "base_event_weight",
                                               "nominal_event_weight",
                                               "in_fiducial",
                                               "mc_n_strange",
                                               "mc_n_pion",
                                               "mc_n_proton",
                                               "interaction_mode_category",
                                               "inclusive_strange_channel_category",
                                               "exclusive_strange_channel_category",
                                               "channel_definition_category",
                                               "is_truth_signal",
                                               "pure_slice_signal",
                                               "pass_pre",
                                               "pass_flash",
                                               "pass_fv",
                                               "pass_mu",
                                               "pass_topo",
                                               "pass_final"};

    return v;
  }
    ColumnCollection common_required_columns_;
    ColumnCollection common_optional_columns_;
    std::map<SampleOrigin, ColumnPlan> origin_column_plans_;
};

}

#endif
