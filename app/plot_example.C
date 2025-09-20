// This ROOT macro is kept in the app directory as a lightweight example that can
// be imitated when scripting custom event reading workflows.

#include "EventReader.h"

#include "TCanvas.h"
#include "TColor.h"
#include "TError.h"
#include "TLatex.h"
#include "TString.h"
#include "TStyle.h"

#include <iostream>
#include <string>

using rarexsec::examples::SampleMetadata;
using rarexsec::examples::EventReader;

//
// Example usage from the ROOT prompt:
// root [0] .L app/plot_example.C
// root [1] plot_example("analysis_snapshot.root", "numi_run1", "reco_nu_energy")
//

void plot_example(const char *file_name = "analysis_snapshot.root", const char *tree_name = "",
                  const char *column = "reco_nu_energy", int bins = 40, double min = 0.0, double max = 4.0,
                  const char *selection = "", const char *weight_column = "") {
    try {
        EventReader reader(file_name);

        std::cout << "Opened event file: " << reader.fileName() << '\n';
        std::cout << "  Total POT: " << reader.totalPOT() << '\n';
        std::cout << "  Total triggers: " << reader.totalTriggers() << '\n';

        if (!reader.samples().empty()) {
            std::cout << "\nSamples in the file:" << '\n';
            for (const SampleMetadata &metadata : reader.samples()) {
                std::cout << "  - " << metadata.tree_name << " (beam=" << metadata.beam
                          << ", period=" << metadata.run_period << ")" << '\n';
                std::cout << "      stage=" << metadata.stage << ", variation=" << metadata.variation
                          << ", origin=" << metadata.origin << '\n';
                std::cout << "      relative_path=" << metadata.relative_path << '\n';
                std::cout << "      POT=" << metadata.pot << ", triggers=" << metadata.triggers << '\n';
            }
        }

        std::string tree_to_plot = (tree_name && tree_name[0]) ? tree_name : std::string{};
        if (tree_to_plot.empty()) {
            auto trees = reader.treeNames();
            if (trees.empty()) {
                std::cerr << "No TTrees were found in " << file_name << std::endl;
                return;
            }
            tree_to_plot = trees.front();
            std::cout << "\nNo tree provided, defaulting to: " << tree_to_plot << '\n';
        }

        const auto sample_view = reader.sample(tree_to_plot);
        const SampleMetadata &chosen_sample = sample_view.metadata();

        std::cout << "\nDrawing histogram from tree: " << chosen_sample.tree_name << '\n';
        std::cout << "  Stage=" << chosen_sample.stage << ", variation=" << chosen_sample.variation
                  << ", POT=" << chosen_sample.pot << ", triggers=" << chosen_sample.triggers << '\n';

        std::string hist_name = "h_" + chosen_sample.tree_name + "_" + column;
        auto hist = sample_view.hist1D(column, hist_name, bins, min, max, selection, weight_column);

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
        label.DrawLatex(0.55, 0.86, Form("Total POT = %.2e", reader.totalPOT()));
        label.DrawLatex(0.55, 0.82, Form("Total triggers = %ld", reader.totalTriggers()));
        label.DrawLatex(0.55, 0.78, Form("Entries = %.0f", hist->GetEntries()));

        const std::string output_name = hist_name + ".png";
        canvas->SaveAs(output_name.c_str());
        std::cout << "\nSaved canvas to " << output_name << std::endl;

    } catch (const std::exception &ex) {
        Error("plot_example", "%s", ex.what());
    }
}
