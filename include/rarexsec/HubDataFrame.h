#ifndef HUB_DATAFRAME_H
#define HUB_DATAFRAME_H

#include <memory>
#include <string>
#include <vector>

#include "ROOT/RDataFrame.hxx"
#include "TChain.h"

namespace proc {

class HubDataFrame {
  public:
    struct EntryInfo {
        UInt_t entry_id = 0U;
        std::string sample_key;
        std::string beam;
        std::string period;
        std::string variation;
        std::string origin;
        std::string stage;
        std::string dataset_path;
        std::string dataset_tree;
        std::string friend_path;
        std::string friend_tree;
    };

    explicit HubDataFrame(const std::string &hub_path);

    ROOT::RDF::RNode query(
        const std::string &beam,
        const std::string &period,
        const std::string &variation = "nominal",
        const std::string &origin = "",
        const std::string &stage = ""
    );

    ROOT::RDF::RNode getNominal(const std::string &beam, const std::string &period, const std::string &stage = "") {
        return query(beam, period, "nominal", "", stage);
    }

    ROOT::RDF::RNode getVariation(const std::string &beam, const std::string &period,
                                  const std::string &variation,
                                  const std::string &stage = "",
                                  const std::string &origin = "") {
        return query(beam, period, variation, origin, stage);
    }

    const std::vector<EntryInfo> &entries();

    struct Combination {
        std::string sample_key;
        std::string beam;
        std::string period;
        std::string variation;
        std::string origin;
        std::string stage;
    };

    std::vector<Combination> getAllCombinations();

    const std::string &baseDirectory() const noexcept { return base_directory_; }
    const std::string &defaultFriendTreeName() const noexcept { return friend_tree_name_; }

  private:
    std::string hub_path_;
    std::unique_ptr<TChain> current_chain_;
    std::unique_ptr<TChain> friend_chain_;
    std::string base_directory_;
    std::string friend_tree_name_;
    std::vector<EntryInfo> cached_entries_;
    bool entries_loaded_;
};

} // namespace proc

#endif
