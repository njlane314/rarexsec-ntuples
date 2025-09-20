#include "TDirectory.h"
#include "TError.h"
#include "TFile.h"
#include "TTree.h"

#include <string>
#include <vector>

void create_test_sample(const char *filename) {
    TFile file(filename, "RECREATE");
    if (file.IsZombie()) {
        Error("create_test_sample", "Could not create output file %s", filename);
        return;
    }

    TDirectory *dir = file.mkdir("nuselection");
    if (!dir) {
        Error("create_test_sample", "Could not create nuselection directory");
        return;
    }
    dir->cd();

    TTree tree("EventSelectionFilter", "Integration test sample");

    int run = 1001;
    int sub = 2;
    int evt = 3;

    std::vector<float> track_shower_scores{0.95f};
    std::vector<float> trk_llr_pid_v{0.9f};
    std::vector<float> track_length{15.f};
    std::vector<float> track_distance_to_vertex{1.5f};
    std::vector<float> track_start_x{120.f};
    std::vector<float> track_start_y{-10.f};
    std::vector<float> track_start_z{400.f};
    std::vector<float> track_end_x{130.f};
    std::vector<float> track_end_y{-5.f};
    std::vector<float> track_end_z{450.f};
    std::vector<unsigned> pfp_generations{2u};
    std::vector<int> pfp_num_plane_hits_U{2};
    std::vector<int> pfp_num_plane_hits_V{2};
    std::vector<int> pfp_num_plane_hits_Y{2};
    std::vector<float> track_theta{0.4f};

    std::vector<std::string> blip_process{"null"};
    std::vector<float> blip_x{10.f};
    std::vector<float> blip_y{15.f};
    std::vector<float> blip_z{20.f};

    float reco_neutrino_vertex_x = 100.f;
    float reco_neutrino_vertex_y = 5.f;
    float reco_neutrino_vertex_z = 400.f;

    float reco_neutrino_vertex_sce_x = 102.f;
    float reco_neutrino_vertex_sce_y = 6.f;
    float reco_neutrino_vertex_sce_z = 402.f;

    float optical_filter_pe_beam = 50.f;
    float optical_filter_pe_veto = 5.f;
    int num_slices = 1;
    float topological_score = 0.9f;
    float contained_fraction = 0.8f;
    float slice_cluster_fraction = 0.75f;
    int software_trigger = 1;

    tree.Branch("run", &run);
    tree.Branch("sub", &sub);
    tree.Branch("evt", &evt);

    tree.Branch("track_shower_scores", &track_shower_scores);
    tree.Branch("trk_llr_pid_v", &trk_llr_pid_v);
    tree.Branch("track_length", &track_length);
    tree.Branch("track_distance_to_vertex", &track_distance_to_vertex);
    tree.Branch("track_start_x", &track_start_x);
    tree.Branch("track_start_y", &track_start_y);
    tree.Branch("track_start_z", &track_start_z);
    tree.Branch("track_end_x", &track_end_x);
    tree.Branch("track_end_y", &track_end_y);
    tree.Branch("track_end_z", &track_end_z);
    tree.Branch("pfp_generations", &pfp_generations);
    tree.Branch("pfp_num_plane_hits_U", &pfp_num_plane_hits_U);
    tree.Branch("pfp_num_plane_hits_V", &pfp_num_plane_hits_V);
    tree.Branch("pfp_num_plane_hits_Y", &pfp_num_plane_hits_Y);
    tree.Branch("track_theta", &track_theta);

    tree.Branch("blip_process", &blip_process);
    tree.Branch("blip_x", &blip_x);
    tree.Branch("blip_y", &blip_y);
    tree.Branch("blip_z", &blip_z);

    tree.Branch("reco_neutrino_vertex_x", &reco_neutrino_vertex_x);
    tree.Branch("reco_neutrino_vertex_y", &reco_neutrino_vertex_y);
    tree.Branch("reco_neutrino_vertex_z", &reco_neutrino_vertex_z);

    tree.Branch("reco_neutrino_vertex_sce_x", &reco_neutrino_vertex_sce_x);
    tree.Branch("reco_neutrino_vertex_sce_y", &reco_neutrino_vertex_sce_y);
    tree.Branch("reco_neutrino_vertex_sce_z", &reco_neutrino_vertex_sce_z);

    tree.Branch("optical_filter_pe_beam", &optical_filter_pe_beam);
    tree.Branch("optical_filter_pe_veto", &optical_filter_pe_veto);
    tree.Branch("num_slices", &num_slices);
    tree.Branch("topological_score", &topological_score);
    tree.Branch("contained_fraction", &contained_fraction);
    tree.Branch("slice_cluster_fraction", &slice_cluster_fraction);
    tree.Branch("software_trigger", &software_trigger);

    tree.Fill();
    tree.Write("", TObject::kOverwrite);

    dir->cd();
    file.Write("", TObject::kOverwrite);
    file.Close();
}
