#ifndef HUB_DATAFRAME_H
#define HUB_DATAFRAME_H

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "ROOT/RDataFrame.hxx"
#include "TChain.h"

namespace proc {

class HubDataFrame {
  public:
    struct Summary {
        double total_pot = 0.0;
        long total_triggers = 0;
        std::string base_directory;
        std::string resolved_base_directory;
        std::string friend_tree = "meta";
    };

    struct CatalogEntry {
        struct FriendInfo {
            std::string label;
            std::string tree;
            std::string path;
        };

        std::uint32_t entry_id = 0U;
        std::uint32_t sample_id = 0U;
        std::uint16_t beam_id = 0U;
        std::uint16_t period_id = 0U;
        std::uint16_t variation_id = 0U;
        std::uint8_t origin_id = 0U;
        std::string dataset_path;
        std::string dataset_tree;
        std::string friend_path;
        std::string friend_tree;
        std::uint64_t n_events = 0ULL;
        std::uint64_t first_event_uid = 0ULL;
        std::uint64_t last_event_uid = 0ULL;
        double sum_weights = 0.0;
        double pot = 0.0;
        long triggers = 0;
        std::string sample_key;
        std::string beam;
        std::string period;
        std::string variation;
        std::string origin;
        std::string stage;
        std::vector<FriendInfo> friends;
    };

    struct Combination {
        std::string sample_key;
        std::string beam;
        std::string period;
        std::string variation;
        std::string origin;
        std::string stage;
    };

    struct ProvenanceDictionaries {
        std::map<std::string, std::uint32_t> sample_ids;
        std::map<std::string, std::uint16_t> beam_ids;
        std::map<std::string, std::uint16_t> period_ids;
        std::map<std::string, std::uint16_t> stage_ids;
        std::map<std::string, std::uint16_t> variation_ids;
        std::map<std::string, std::uint8_t> origin_ids;
    };

    class Selection {
      public:
        explicit Selection(HubDataFrame &owner);

        Selection &sample(const std::string &value);
        Selection &beam(const std::string &value);
        Selection &period(const std::string &value);
        Selection &variation(const std::string &value);
        Selection &origin(const std::string &value);
        Selection &stage(const std::string &value);

        Selection &clearSample();
        Selection &clearBeam();
        Selection &clearPeriod();
        Selection &clearVariation();
        Selection &clearOrigin();
        Selection &clearStage();

        std::vector<const CatalogEntry *> entries() const;
        ROOT::RDF::RNode load();

      private:
        HubDataFrame &owner_;
        std::optional<std::string> sample_;
        std::optional<std::string> beam_;
        std::optional<std::string> period_;
        std::optional<std::string> variation_;
        std::optional<std::string> origin_;
        std::optional<std::string> stage_;
    };

    explicit HubDataFrame(const std::string &hub_path);

    Selection select();

    ROOT::RDF::RNode query(const std::string &beam, const std::string &period,
                           const std::string &variation = "nominal", const std::string &origin = "",
                           const std::string &stage = "");

    ROOT::RDF::RNode getNominal(const std::string &beam, const std::string &period,
                                const std::string &stage = "") {
        return query(beam, period, "nominal", "", stage);
    }

    ROOT::RDF::RNode getVariation(const std::string &beam, const std::string &period,
                                  const std::string &variation,
                                  const std::string &stage = "",
                                  const std::string &origin = "") {
        return query(beam, period, variation, origin, stage);
    }

    const Summary &summary() const noexcept { return summary_; }
    const std::vector<CatalogEntry> &catalog() const noexcept { return entries_; }
    const ProvenanceDictionaries &provenance() const noexcept { return provenance_dicts_; }

    std::vector<Combination> getAllCombinations() const;

    std::vector<std::string> beams() const;
    std::vector<std::string> periods(const std::optional<std::string> &beam = std::nullopt) const;
    std::vector<std::string> origins(const std::optional<std::string> &beam = std::nullopt,
                                     const std::optional<std::string> &period = std::nullopt,
                                     const std::optional<std::string> &stage = std::nullopt) const;
    std::vector<std::string> stages(const std::optional<std::string> &beam = std::nullopt,
                                    const std::optional<std::string> &period = std::nullopt) const;
    std::vector<std::string> variations(const std::optional<std::string> &beam = std::nullopt,
                                        const std::optional<std::string> &period = std::nullopt,
                                        const std::optional<std::string> &origin = std::nullopt,
                                        const std::optional<std::string> &stage = std::nullopt) const;
    std::vector<std::string> sampleKeys(const std::optional<std::string> &beam = std::nullopt,
                                        const std::optional<std::string> &period = std::nullopt,
                                        const std::optional<std::string> &stage = std::nullopt,
                                        const std::optional<std::string> &variation = std::nullopt) const;

    void setBaseDirectoryOverride(const std::filesystem::path &path);
    void clearBaseDirectoryOverride();
    std::filesystem::path resolvedBaseDirectory() const;

  private:
    std::vector<const CatalogEntry *> resolveEntries(const std::optional<std::string> &sample,
                                                     const std::optional<std::string> &beam,
                                                     const std::optional<std::string> &period,
                                                     const std::optional<std::string> &variation,
                                                     const std::optional<std::string> &origin,
                                                     const std::optional<std::string> &stage) const;
    ROOT::RDF::RNode loadSelection(const std::optional<std::string> &sample, const std::optional<std::string> &beam,
                                   const std::optional<std::string> &period, const std::optional<std::string> &variation,
                                   const std::optional<std::string> &origin, const std::optional<std::string> &stage);
    ROOT::RDF::RNode buildDataFrame(const std::vector<const CatalogEntry *> &entries);

    std::filesystem::path resolveDatasetPath(const CatalogEntry &entry) const;
    std::filesystem::path resolveFriendPath(const CatalogEntry &entry) const;
    std::filesystem::path resolveFriendPath(const std::string &friend_path) const;

    void loadMetadata();
    void loadCatalog();
    void loadFriendMetadata();

    std::string hub_path_;
    std::string hub_directory_;
    std::unique_ptr<TChain> current_chain_;
    struct FriendChain {
        std::unique_ptr<TChain> chain;
        std::string alias;
        std::string key;
    };
    std::vector<FriendChain> friend_chains_; //! transient helper chains, not part of the ROOT dictionary
    Summary summary_;
    std::vector<CatalogEntry> entries_;
    ProvenanceDictionaries provenance_dicts_;
    std::optional<std::string> base_directory_override_;
};

} // namespace proc

#if defined(RAREXSEC_HEADER_ONLY) && RAREXSEC_HEADER_ONLY
#include <rarexsec/detail/HubDataFrameImpl.h>
#endif

#endif
