#include "TFile.h"
#include "TTree.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

namespace {

struct SampleExpectation {
    std::string period;
    std::string sample_key;
};

struct RunResult {
    int exit_code{0};
    double seconds{0.0};
};

std::filesystem::path makeTemporaryDirectory() {
    namespace fs = std::filesystem;
    const auto base = fs::temp_directory_path();
    for (int attempt = 0; attempt < 10; ++attempt) {
        const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
        const auto candidate = base /
                               ("rarexsec-regression-" + std::to_string(timestamp) + "-" + std::to_string(attempt));
        if (!fs::exists(candidate)) {
            fs::create_directories(candidate);
            return candidate;
        }
    }
    throw std::runtime_error("Failed to create a temporary directory for the regression test");
}

void writeSampleNtuple(const std::filesystem::path &path) {
    namespace fs = std::filesystem;
    fs::create_directories(path.parent_path());

    TFile file(path.c_str(), "RECREATE");
    if (file.IsZombie()) {
        throw std::runtime_error("Failed to create sample input ntuple");
    }

    file.mkdir("nuselection");
    file.cd("nuselection");

    TTree tree("EventSelectionFilter", "EventSelectionFilter");

    int run = 12001;
    int sub = 3;
    int evt = 42;

    float reco_vtx_x = 110.f;
    float reco_vtx_y = 0.5f;
    float reco_vtx_z = 200.f;

    float reco_vtx_sce_x = 112.f;
    float reco_vtx_sce_y = 0.9f;
    float reco_vtx_sce_z = 205.f;

    std::vector<std::string> blip_process{"null"};
    std::vector<float> blip_x{1.0f};
    std::vector<float> blip_y{2.0f};
    std::vector<float> blip_z{3.0f};

    std::vector<float> track_shower_scores{0.95f};
    std::vector<float> track_llr{0.3f};
    std::vector<float> track_length{12.0f};
    std::vector<float> track_distance{2.0f};

    std::vector<float> track_start_x{120.f};
    std::vector<float> track_start_y{0.4f};
    std::vector<float> track_start_z{210.f};

    std::vector<float> track_end_x{130.f};
    std::vector<float> track_end_y{0.6f};
    std::vector<float> track_end_z{230.f};

    std::vector<float> track_theta{1.2f};

    std::vector<unsigned> pfp_generations{2u};
    std::vector<int> pfp_hits_u{1};
    std::vector<int> pfp_hits_v{1};
    std::vector<int> pfp_hits_y{1};

    float optical_filter_pe_beam = 25.f;
    float optical_filter_pe_veto = 5.f;
    int num_slices = 1;
    float topological_score = 0.7f;
    float contained_fraction = 0.9f;
    float slice_cluster_fraction = 0.8f;
    int software_trigger = 1;

    std::vector<float> detector_image_u{0.1f, 0.2f};
    std::vector<float> detector_image_v{0.3f, 0.4f};
    std::vector<float> detector_image_w{0.5f, 0.6f};

    std::vector<float> semantic_image_u{0.7f, 0.8f};
    std::vector<float> semantic_image_v{0.9f, 1.0f};
    std::vector<float> semantic_image_w{1.1f, 1.2f};

    std::vector<float> event_detector_image_u{0.3f};
    std::vector<float> event_detector_image_v{0.4f};
    std::vector<float> event_detector_image_w{0.5f};

    std::vector<float> event_semantic_image_u{0.6f};
    std::vector<float> event_semantic_image_v{0.7f};
    std::vector<float> event_semantic_image_w{0.8f};

    std::vector<float> event_adc_u{10.f};
    std::vector<float> event_adc_v{11.f};
    std::vector<float> event_adc_w{12.f};

    std::vector<int> slice_semantic_counts_u{1};
    std::vector<int> slice_semantic_counts_v{2};
    std::vector<int> slice_semantic_counts_w{3};

    std::vector<int> event_semantic_counts_u{4};
    std::vector<int> event_semantic_counts_v{5};
    std::vector<int> event_semantic_counts_w{6};

    bool is_vtx_in_image_u = true;
    bool is_vtx_in_image_v = true;
    bool is_vtx_in_image_w = true;

    tree.Branch("run", &run);
    tree.Branch("sub", &sub);
    tree.Branch("evt", &evt);
    tree.Branch("reco_neutrino_vertex_x", &reco_vtx_x);
    tree.Branch("reco_neutrino_vertex_y", &reco_vtx_y);
    tree.Branch("reco_neutrino_vertex_z", &reco_vtx_z);
    tree.Branch("reco_neutrino_vertex_sce_x", &reco_vtx_sce_x);
    tree.Branch("reco_neutrino_vertex_sce_y", &reco_vtx_sce_y);
    tree.Branch("reco_neutrino_vertex_sce_z", &reco_vtx_sce_z);

    tree.Branch("blip_process", &blip_process);
    tree.Branch("blip_x", &blip_x);
    tree.Branch("blip_y", &blip_y);
    tree.Branch("blip_z", &blip_z);

    tree.Branch("track_shower_scores", &track_shower_scores);
    tree.Branch("trk_llr_pid_v", &track_llr);
    tree.Branch("track_length", &track_length);
    tree.Branch("track_distance_to_vertex", &track_distance);
    tree.Branch("track_start_x", &track_start_x);
    tree.Branch("track_start_y", &track_start_y);
    tree.Branch("track_start_z", &track_start_z);
    tree.Branch("track_end_x", &track_end_x);
    tree.Branch("track_end_y", &track_end_y);
    tree.Branch("track_end_z", &track_end_z);
    tree.Branch("track_theta", &track_theta);

    tree.Branch("pfp_generations", &pfp_generations);
    tree.Branch("pfp_num_plane_hits_U", &pfp_hits_u);
    tree.Branch("pfp_num_plane_hits_V", &pfp_hits_v);
    tree.Branch("pfp_num_plane_hits_Y", &pfp_hits_y);

    tree.Branch("optical_filter_pe_beam", &optical_filter_pe_beam);
    tree.Branch("optical_filter_pe_veto", &optical_filter_pe_veto);
    tree.Branch("num_slices", &num_slices);
    tree.Branch("topological_score", &topological_score);
    tree.Branch("contained_fraction", &contained_fraction);
    tree.Branch("slice_cluster_fraction", &slice_cluster_fraction);
    tree.Branch("software_trigger", &software_trigger);

    tree.Branch("detector_image_u", &detector_image_u);
    tree.Branch("detector_image_v", &detector_image_v);
    tree.Branch("detector_image_w", &detector_image_w);

    tree.Branch("semantic_image_u", &semantic_image_u);
    tree.Branch("semantic_image_v", &semantic_image_v);
    tree.Branch("semantic_image_w", &semantic_image_w);

    tree.Branch("event_detector_image_u", &event_detector_image_u);
    tree.Branch("event_detector_image_v", &event_detector_image_v);
    tree.Branch("event_detector_image_w", &event_detector_image_w);

    tree.Branch("event_semantic_image_u", &event_semantic_image_u);
    tree.Branch("event_semantic_image_v", &event_semantic_image_v);
    tree.Branch("event_semantic_image_w", &event_semantic_image_w);

    tree.Branch("event_adc_u", &event_adc_u);
    tree.Branch("event_adc_v", &event_adc_v);
    tree.Branch("event_adc_w", &event_adc_w);

    tree.Branch("slice_semantic_counts_u", &slice_semantic_counts_u);
    tree.Branch("slice_semantic_counts_v", &slice_semantic_counts_v);
    tree.Branch("slice_semantic_counts_w", &slice_semantic_counts_w);

    tree.Branch("event_semantic_counts_u", &event_semantic_counts_u);
    tree.Branch("event_semantic_counts_v", &event_semantic_counts_v);
    tree.Branch("event_semantic_counts_w", &event_semantic_counts_w);

    tree.Branch("is_vtx_in_image_u", &is_vtx_in_image_u);
    tree.Branch("is_vtx_in_image_v", &is_vtx_in_image_v);
    tree.Branch("is_vtx_in_image_w", &is_vtx_in_image_w);

    tree.Fill();

    file.Write("", TObject::kOverwrite);
    file.Close();
}

std::vector<SampleExpectation> writeRunConfig(const std::filesystem::path &config_path,
                                              const std::filesystem::path &base_dir,
                                              std::string_view relative_input_path,
                                              int run_count,
                                              int samples_per_run) {
    using json = nlohmann::json;
    json config;
    config["ntuple_base_directory"] = base_dir.string();

    json beam_runs = json::object();
    std::vector<SampleExpectation> expectations;
    expectations.reserve(static_cast<std::size_t>(run_count * samples_per_run));

    for (int run_index = 0; run_index < run_count; ++run_index) {
        const std::string run_label = "run" + std::to_string(run_index + 1);
        json samples = json::array();
        for (int sample_index = 0; sample_index < samples_per_run; ++sample_index) {
            const std::string sample_key =
                "ext_sample_" + run_label + "_sample" + std::to_string(sample_index);
            json sample_json;
            sample_json["sample_key"] = sample_key;
            sample_json["sample_type"] = "ext";
            sample_json["relative_path"] = std::string(relative_input_path);
            sample_json["stage_name"] = "selection_ext";
            sample_json["triggers"] = 100 + sample_index;
            samples.push_back(sample_json);

            expectations.push_back({run_label, sample_key});
        }

        json run_json;
        run_json["ext_triggers"] = 1000 + run_index;
        run_json["samples"] = samples;
        beam_runs[run_label] = run_json;
    }

    json run_configs;
    run_configs["test-beam"] = beam_runs;
    config["run_configurations"] = run_configs;

    std::ofstream out(config_path);
    out << config.dump(2) << std::endl;

    return expectations;
}

std::string buildPeriodArgument(int run_count) {
    if (run_count <= 1) {
        return "run1";
    }
    std::ostringstream os;
    os << "run1-run" << run_count;
    return os.str();
}

RunResult executeSnapshot(const std::filesystem::path &executable,
                          const std::filesystem::path &config_path,
                          const std::string &period_argument,
                          const std::filesystem::path &output_path) {
    std::ostringstream command;
    command << '"' << executable.string() << '"';
    command << ' ' << '"' << config_path.string() << '"';
    command << " test-beam " << period_argument << " 1 ";
    command << '"' << output_path.string() << '"';

    const auto start = std::chrono::steady_clock::now();
    const int exit_code = std::system(command.str().c_str());
    const auto finish = std::chrono::steady_clock::now();

    RunResult result;
    result.exit_code = exit_code;
    result.seconds = std::chrono::duration<double>(finish - start).count();
    return result;
}

long readTreeEntries(const std::filesystem::path &output_path, const SampleExpectation &expectation) {
    const std::string tree_path = "samples/test-beam/" + expectation.period +
                                  "/ext/selection_ext/" + expectation.sample_key + "/nominal/events";

    std::unique_ptr<TFile> file{TFile::Open(output_path.c_str(), "READ")};
    if (!file || file->IsZombie()) {
        throw std::runtime_error("Failed to open snapshot output file");
    }

    TTree *tree = dynamic_cast<TTree *>(file->Get(tree_path.c_str()));
    if (!tree) {
        throw std::runtime_error("Expected tree missing at path: " + tree_path);
    }

    return tree->GetEntries();
}

void ensure(bool condition, const std::string &message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

} // namespace

int main(int argc, char **argv) {
    try {
        ensure(argc >= 2, "Path to snapshot-analysis executable not provided");

        const std::filesystem::path snapshot_executable = argv[1];
        ensure(std::filesystem::exists(snapshot_executable), "snapshot-analysis executable not found");

        constexpr int kRunCount = 5;
        constexpr int kSamplesPerRun = 8;

        const auto temp_dir = makeTemporaryDirectory();
        const auto ntuple_dir = temp_dir / "ntuples";
        std::filesystem::create_directories(ntuple_dir);

        const auto input_path = ntuple_dir / "sample.root";
        writeSampleNtuple(input_path);

        const auto config_path = temp_dir / "config.json";
        const auto expectations =
            writeRunConfig(config_path, ntuple_dir, input_path.filename().string(), kRunCount, kSamplesPerRun);

        const auto output_path = temp_dir / "snapshot_output.root";
        const std::string period_argument = buildPeriodArgument(kRunCount);

        const auto first_result = executeSnapshot(snapshot_executable, config_path, period_argument, output_path);
        ensure(first_result.exit_code == 0, "First snapshot-analysis invocation failed");

        std::vector<long> baseline_entries;
        baseline_entries.reserve(expectations.size());
        for (const auto &expectation : expectations) {
            baseline_entries.push_back(readTreeEntries(output_path, expectation));
        }

        const auto second_result = executeSnapshot(snapshot_executable, config_path, period_argument, output_path);
        ensure(second_result.exit_code == 0, "Second snapshot-analysis invocation failed");

        for (std::size_t i = 0; i < expectations.size(); ++i) {
            const long entries = readTreeEntries(output_path, expectations[i]);
            ensure(entries == baseline_entries[i],
                   "Tree entry count changed after second run for " + expectations[i].sample_key);
        }

        ensure(std::filesystem::remove(output_path), "Failed to remove snapshot output for baseline comparison");

        const auto baseline_result = executeSnapshot(snapshot_executable, config_path, period_argument, output_path);
        ensure(baseline_result.exit_code == 0, "Baseline snapshot-analysis run failed after resetting output");

        for (std::size_t i = 0; i < expectations.size(); ++i) {
            const long entries = readTreeEntries(output_path, expectations[i]);
            ensure(entries == baseline_entries[i],
                   "Tree entry count changed after recreating output for " + expectations[i].sample_key);
        }

        const double baseline_reference = std::min(first_result.seconds, baseline_result.seconds);
        const double epsilon = 1e-3;
        std::ostringstream timing_report;
        timing_report << "First run: " << first_result.seconds << "s, second run: " << second_result.seconds
                      << "s, recreated baseline: " << baseline_result.seconds << "s";
        ensure(second_result.seconds + epsilon < baseline_reference,
               "Second run did not improve over recreating output. " + timing_report.str());

        std::cout << timing_report.str() << std::endl;
        return 0;
    } catch (const std::exception &ex) {
        std::cerr << "snapshot-analysis regression test failed: " << ex.what() << std::endl;
        return 1;
    }
}
