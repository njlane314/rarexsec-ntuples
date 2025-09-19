#ifndef RAREXSEC_EXAMPLES_SNAPSHOTPLOTINTERFACE_H
#define RAREXSEC_EXAMPLES_SNAPSHOTPLOTINTERFACE_H

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RDF/TH1DModel.hxx>

#include <Rtypes.h>
#include <TDirectory.h>
#include <TFile.h>
#include <TTree.h>
#include <TTreeReader.h>
#include <TTreeReaderValue.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "TH1D.h"

namespace rarexsec::examples {

/**
 * @brief Summary information for each processed sample stored in the snapshot file.
 */
struct SampleSummary {
    std::string tree_name;
    std::string beam;
    std::string run_period;
    std::string dataset_id;
    std::string relative_path;
    std::string variation;
    std::string stage;
    std::string origin;
    double pot = 0.0;
    long triggers = 0;
};

/**
 * @brief Utility class that reads metadata from a rarexsec snapshot ROOT file and provides
 *        helpers to build ROOT::RDataFrame driven histograms.
 */
class SnapshotPlotInterface {
  public:
    /**
     * @brief Lightweight proxy providing convenience methods for a single snapshot sample.
     */
    class SampleHandle {
      public:
        const SampleSummary &summary() const { return *summary_; }

        const std::string &treeName() const { return summary_->tree_name; }

        ROOT::RDF::RDataFrame dataFrame() const { return owner_->dataFrame(treeName()); }

        ROOT::RDF::RResultPtr<TH1D> hist1D(const std::string &column, const ROOT::RDF::TH1DModel &model,
                                           const std::string &selection = "",
                                           const std::string &weight_column = "") const {
            return owner_->makeHistogram(treeName(), model, column, selection, weight_column);
        }

        ROOT::RDF::RResultPtr<TH1D> hist1D(const std::string &column, const std::string &hist_name, int bins,
                                           double min, double max, const std::string &selection = "",
                                           const std::string &weight_column = "") const {
            return owner_->makeHistogram(treeName(), column, hist_name, bins, min, max, selection, weight_column);
        }

      private:
        SampleHandle(const SnapshotPlotInterface *owner, const SampleSummary *summary)
            : owner_(owner), summary_(summary) {}

        const SnapshotPlotInterface *owner_ = nullptr;
        const SampleSummary *summary_ = nullptr;

        friend class SnapshotPlotInterface;
    };

    explicit SnapshotPlotInterface(std::string file_name) : file_name_(std::move(file_name)) { loadMetadata(); }

    const std::string &fileName() const { return file_name_; }

    double totalPOT() const { return total_pot_; }

    long totalTriggers() const { return total_triggers_; }

    const std::vector<SampleSummary> &samples() const { return samples_; }

    auto begin() const { return samples_.begin(); }
    auto end() const { return samples_.end(); }

    std::vector<std::string> treeNames() const {
        std::vector<std::string> names;
        names.reserve(samples_.size());
        for (const auto &summary : samples_) {
            names.emplace_back(summary.tree_name);
        }
        return names;
    }

    /**
     * @brief Return a handle for the requested sample if it exists.
     */
    std::optional<SampleHandle> trySample(const std::string &tree_name) const {
        auto it = sample_index_.find(tree_name);
        if (it == sample_index_.end()) {
            return std::nullopt;
        }
        return SampleHandle(this, &samples_.at(it->second));
    }

    /**
     * @brief Return a handle for the requested sample or throw if it is missing.
     */
    SampleHandle sample(const std::string &tree_name) const {
        auto handle = trySample(tree_name);
        if (!handle) {
            throw std::runtime_error("SnapshotPlotInterface: unknown tree '" + tree_name + "' in " + file_name_);
        }
        return *handle;
    }

    /**
     * @brief Construct an RDataFrame for the requested snapshot tree.
     */
    ROOT::RDF::RDataFrame dataFrame(const std::string &tree_name) const {
        requireSample(tree_name);
        return ROOT::RDF::RDataFrame(tree_name, file_name_);
    }

    /**
     * @brief Histogram a branch using a preconfigured TH1D model.
     */
    ROOT::RDF::RResultPtr<TH1D> makeHistogram(const std::string &tree_name, const ROOT::RDF::TH1DModel &model,
                                              const std::string &column, const std::string &selection = "",
                                              const std::string &weight_column = "") const {
        auto df = dataFrame(tree_name);
        if (selection.empty()) {
            if (weight_column.empty()) {
                return df.Histo1D(model, column);
            }
            return df.Histo1D(model, column, weight_column);
        }

        auto filtered = df.Filter(selection, "selection");
        if (weight_column.empty()) {
            return filtered.Histo1D(model, column);
        }
        return filtered.Histo1D(model, column, weight_column);
    }

    /**
     * @brief Histogram a branch with a simple uniform bin definition.
     */
    ROOT::RDF::RResultPtr<TH1D> makeHistogram(const std::string &tree_name, const std::string &column,
                                              const std::string &hist_name, int bins, double min, double max,
                                              const std::string &selection = "",
                                              const std::string &weight_column = "") const {
        ROOT::RDF::TH1DModel model(hist_name.c_str(), column.c_str(), bins, min, max);
        return makeHistogram(tree_name, model, column, selection, weight_column);
    }

  private:
    const SampleSummary &requireSample(const std::string &tree_name) const {
        auto it = sample_index_.find(tree_name);
        if (it == sample_index_.end()) {
            throw std::runtime_error("SnapshotPlotInterface: unknown tree '" + tree_name + "' in " + file_name_);
        }
        return samples_.at(it->second);
    }

    void loadMetadata() {
        std::unique_ptr<TFile> file{TFile::Open(file_name_.c_str(), "READ")};
        if (!file || file->IsZombie()) {
            throw std::runtime_error("SnapshotPlotInterface: unable to open file " + file_name_);
        }

        TDirectory *meta_dir = file->GetDirectory("meta");
        if (!meta_dir) {
            throw std::runtime_error("SnapshotPlotInterface: missing 'meta' directory in " + file_name_);
        }

        total_pot_ = 0.0;
        total_triggers_ = 0;
        samples_.clear();
        sample_index_.clear();

        if (auto *totals_tree = dynamic_cast<TTree *>(meta_dir->Get("totals")); totals_tree) {
            TTreeReader totals_reader(totals_tree);
            TTreeReaderValue<double> total_pot(totals_reader, "total_pot");
            TTreeReaderValue<Long64_t> total_triggers(totals_reader, "total_triggers");

            if (totals_reader.Next()) {
                total_pot_ = *total_pot;
                total_triggers_ = static_cast<long>(*total_triggers);
            }
        }

        if (auto *samples_tree = dynamic_cast<TTree *>(meta_dir->Get("samples")); samples_tree) {
            TTreeReader samples_reader(samples_tree);
            TTreeReaderValue<std::string> tree_name(samples_reader, "tree_name");
            TTreeReaderValue<std::string> beam(samples_reader, "beam");
            TTreeReaderValue<std::string> run_period(samples_reader, "run_period");
            TTreeReaderValue<std::string> dataset_id(samples_reader, "dataset_id");
            TTreeReaderValue<std::string> relative_path(samples_reader, "relative_path");
            TTreeReaderValue<std::string> variation(samples_reader, "variation");
            TTreeReaderValue<std::string> stage_name(samples_reader, "stage_name");
            TTreeReaderValue<std::string> origin(samples_reader, "origin");
            TTreeReaderValue<double> sample_pot(samples_reader, "sample_pot");
            TTreeReaderValue<Long64_t> sample_triggers(samples_reader, "sample_triggers");

            std::vector<SampleSummary> samples;
            samples.reserve(static_cast<std::size_t>(samples_tree->GetEntries()));
            while (samples_reader.Next()) {
                samples.push_back(SampleSummary{*tree_name,   *beam,      *run_period, *dataset_id, *relative_path,
                                                *variation, *stage_name, *origin,     *sample_pot,
                                                static_cast<long>(*sample_triggers)});
            }

            std::sort(samples.begin(), samples.end(),
                      [](const SampleSummary &a, const SampleSummary &b) { return a.tree_name < b.tree_name; });

            samples_.swap(samples);
            sample_index_.reserve(samples_.size());
            for (std::size_t idx = 0; idx < samples_.size(); ++idx) {
                sample_index_.emplace(samples_[idx].tree_name, idx);
            }
        }
    }

    std::string file_name_;
    double total_pot_ = 0.0;
    long total_triggers_ = 0;
    std::vector<SampleSummary> samples_;
    std::unordered_map<std::string, std::size_t> sample_index_;
};

} // namespace rarexsec::examples

#endif // RAREXSEC_EXAMPLES_SNAPSHOTPLOTINTERFACE_H
