#ifndef HUB_DATAFRAME_H
#define HUB_DATAFRAME_H

#include <memory>
#include <string>

#include "ROOT/RDataFrame.hxx"
#include "TChain.h"

namespace proc {

class HubDataFrame {
  public:
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

  private:
    std::string hub_path_;
    std::unique_ptr<TChain> current_chain_;
    std::unique_ptr<TChain> friend_chain_;
    std::string base_directory_;
    std::string friend_tree_name_;
};

} // namespace proc

#endif
