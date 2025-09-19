// This ROOT macro is kept in the apps directory as a lightweight example that can
// be imitated when scripting custom snapshot exploration workflows.

#include "ExampleInterface.h"

#include "TCanvas.h"
#include "TColor.h"
#include "TError.h"
#include "TLatex.h"
#include "TString.h"
#include "TStyle.h"

#include <iostream>
#include <string>

using rarexsec::examples::ExampleInterface;
using rarexsec::examples::SampleSummary;

//
// Example usage from the ROOT prompt:
// root [0] .L apps/plot_example.C
// root [1] plot_example("analysis_snapshot.root", "numi_run1", "reco_nu_energy")
//

void plot_example(const char *file_name = "analysis_snapshot.root", const char *tree_name = "",
                  const char *column = "reco_nu_energy", int bins = 40, double min = 0.0, double max = 4.0,
                  const char *selection = "", const char *weight_column = "") {
    try {
        ExampleInterface snapshot(file_name);

        std::cout << "Opened snapshot file: " << snapshot.fileName() << '\n';
        std::cout << "  Total POT: " << snapshot.totalPOT() << '\n';
        std::cout << "  Total triggers: " << snapshot.totalTriggers() << '\n';

        if (!snapshot.samples().empty()) {
            std::cout << "\nSamples in the file:" << '\n';
            for (const SampleSummary &summary : snapshot.samples()) {
                std::cout << "  - " << summary.tree_name << " (beam=" << summary.beam << ", period="
                          << summary.run_period << ", dataset=" << summary.dataset_id << ")" << '\n';
                std::cout << "      stage=" << summary.stage << ", variation=" << summary.variation
                          << ", origin=" << summary.origin << '\n';
                std::cout << "      relative_path=" << summary.relative_path << '\n';
                std::cout << "      POT=" << summary.pot << ", triggers=" << summary.triggers << '\n';
            }
        }

        std::string tree_to_plot = (tree_name && tree_name[0]) ? tree_name : std::string{};
        if (tree_to_plot.empty()) {
            auto trees = snapshot.treeNames();
            if (trees.empty()) {
                std::cerr << "No TTrees were found in " << file_name << std::endl;
                return;
            }
            tree_to_plot = trees.front();
            std::cout << "\nNo tree provided, defaulting to: " << tree_to_plot << '\n';
        }

        const auto sample_handle = snapshot.sample(tree_to_plot);
        const SampleSummary &chosen_sample = sample_handle.summary();

        std::cout << "\nDrawing histogram from tree: " << chosen_sample.tree_name << '\n';
        std::cout << "  Stage=" << chosen_sample.stage << ", variation=" << chosen_sample.variation
                  << ", POT=" << chosen_sample.pot << ", triggers=" << chosen_sample.triggers << '\n';

        std::string hist_name = "h_" + chosen_sample.tree_name + "_" + column;
        auto hist = sample_handle.hist1D(column, hist_name, bins, min, max, selection, weight_column);

        gStyle->SetOptStat(0);
        TCanvas *canvas = new TCanvas(("c_" + tree_to_plot).c_str(), hist_name.c_str(), 900, 650);
        canvas->SetGrid();

        hist->SetDirectory(nullptr);
        hist->SetLineColor(kAzure + 2);
        hist->SetLineWidth(2);
        hist->Draw("HIST");

        TLatex label;
        label.SetNDC();
        label.SetTextSize(0.03);
        label.DrawLatex(0.55, 0.86, Form("Total POT = %.2e", snapshot.totalPOT()));
        label.DrawLatex(0.55, 0.82, Form("Total triggers = %ld", snapshot.totalTriggers()));
        label.DrawLatex(0.55, 0.78, Form("Entries = %.0f", hist->GetEntries()));

        const std::string output_name = hist_name + ".png";
        canvas->SaveAs(output_name.c_str());
        std::cout << "\nSaved canvas to " << output_name << std::endl;

    } catch (const std::exception &ex) {
        Error("plot_example", "%s", ex.what());
    }
}
