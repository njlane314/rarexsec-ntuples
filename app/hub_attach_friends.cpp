#include <rarexsec/FriendWriter.h>
#include <rarexsec/HubCatalog.h>
#include <rarexsec/HubDataFrame.h>
#include <rarexsec/LoggerUtils.h>

#include "TBranch.h"
#include "TFile.h"
#include "TLeaf.h"
#include "TTree.h"
#include "TTreeReader.h"
#include "TTreeReaderValue.h"
#include "Rtypes.h"
#include <RVersion.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

using ColumnOverride = std::pair<std::string, std::string>;

struct ColumnSpec {
    enum class ValueType { Float, Double };

    std::string input_name;
    std::string output_name;
    EDataType leaf_type = kNoType_t;
    ValueType value_type = ValueType::Double;
};

struct ScoreTable {
    std::vector<ColumnSpec> columns;
    std::unordered_map<ULong64_t, std::vector<double>> values;
    std::size_t total_rows = 0;
    std::size_t duplicate_uids = 0;
};

struct Options {
    bool show_help = false;
    std::string hub_path;
    std::string scores_path;
    std::string scores_tree;
    std::string label;
    std::string friend_tree;
    std::filesystem::path output_dir;
    std::vector<ColumnOverride> column_overrides;
};

std::string sanitiseComponent(const std::string &value) {
    if (value.empty()) {
        return std::string{"none"};
    }
    std::string result;
    result.reserve(value.size());
    for (char ch : value) {
        const unsigned char uc = static_cast<unsigned char>(ch);
        if (std::isalnum(uc) || ch == '_' || ch == '-') {
            result.push_back(static_cast<char>(uc));
        } else {
            result.push_back('_');
        }
    }
    if (result.empty()) {
        return std::string{"none"};
    }
    return result;
}

std::string trim(const std::string &value) {
    const auto begin = value.find_first_not_of(" \t");
    if (begin == std::string::npos) {
        return {};
    }
    const auto end = value.find_last_not_of(" \t");
    return value.substr(begin, end - begin + 1);
}

void printUsage() {
    std::cout << "Usage: hub-attach-friends --hub <hub> --scores <scores.root> --tree <tree> --label <label>"
              << " [--friend-tree <name>] [--output-dir <dir>] [--columns a,b,c]" << std::endl;
    std::cout << "\nOptions:" << std::endl;
    std::cout << "  --hub           Path to the hub catalogue (.hub.root)" << std::endl;
    std::cout << "  --scores        ROOT file containing CNN scores" << std::endl;
    std::cout << "  --tree          TTree within the score file (for example cnn_output)" << std::endl;
    std::cout << "  --label         Alias used when attaching the friend (e.g. cnn)" << std::endl;
    std::cout << "  --friend-tree   Optional name for the friend TTree (defaults to --label)" << std::endl;
    std::cout << "  --output-dir    Output directory for friend shards (relative paths are resolved against the hub)"
              << std::endl;
    std::cout << "  --columns       Comma-separated list of score branches (use input or input:output to rename)." << std::endl;
    std::cout << "                   When omitted, all floating-point score columns are attached automatically." << std::endl;
}

Options parseOptions(int argc, char **argv) {
    Options opts;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            opts.show_help = true;
            return opts;
        }
        auto require_value = [&](const char *name) -> std::string {
            if (i + 1 >= argc) {
                throw std::runtime_error(std::string{"Missing value for "} + name);
            }
            return std::string{argv[++i]};
        };

        if (arg == "--hub") {
            opts.hub_path = trim(require_value("--hub"));
        } else if (arg == "--scores") {
            opts.scores_path = trim(require_value("--scores"));
        } else if (arg == "--tree") {
            opts.scores_tree = trim(require_value("--tree"));
        } else if (arg == "--label") {
            opts.label = trim(require_value("--label"));
        } else if (arg == "--friend-tree") {
            opts.friend_tree = trim(require_value("--friend-tree"));
        } else if (arg == "--output-dir") {
            opts.output_dir = std::filesystem::path{trim(require_value("--output-dir"))};
        } else if (arg == "--columns") {
            std::string list = require_value("--columns");
            std::size_t start = 0U;
            while (start < list.size()) {
                auto end = list.find(',', start);
                const auto token_raw = list.substr(start, end == std::string::npos ? std::string::npos : end - start);
                const auto token = trim(token_raw);
                if (!token.empty()) {
                    auto colon = token.find(':');
                    if (colon == std::string::npos) {
                        opts.column_overrides.emplace_back(token, token);
                    } else {
                        const std::string input = trim(token.substr(0, colon));
                        const std::string output = trim(token.substr(colon + 1));
                        if (input.empty() || output.empty()) {
                            throw std::runtime_error("Column override must not be empty");
                        }
                        opts.column_overrides.emplace_back(input, output);
                    }
                }
                if (end == std::string::npos) {
                    break;
                }
                start = end + 1;
            }
        } else {
            throw std::runtime_error("Unrecognised option: " + arg);
        }
    }
    return opts;
}

EDataType getLeafType(const TLeaf *leaf) {
    if (!leaf) {
        return kNoType_t;
    }

#if defined(ROOT_VERSION_CODE) && ROOT_VERSION_CODE >= ROOT_VERSION(6, 30, 0)
    return leaf->GetType();
#else
    const char *type_name = leaf->GetTypeName();
    if (!type_name) {
        return kNoType_t;
    }

    const std::string_view type_view{type_name};
    if (type_view == "Float_t") {
        return kFloat_t;
    }
#if defined(kFloat16_t) || defined(kFloat16Alt_t)
    if (type_view == "Float16_t") {
#if defined(kFloat16_t)
        return kFloat16_t;
#else
        return kFloat16Alt_t;
#endif
    }
#endif
    if (type_view == "Double_t") {
        return kDouble_t;
    }
    if (type_view == "Double32_t") {
        return kDouble32_t;
    }
    if (type_view == "Int_t") {
        return kInt_t;
    }
    if (type_view == "UInt_t") {
        return kUInt_t;
    }
    if (type_view == "Long64_t") {
        return kLong64_t;
    }
    if (type_view == "ULong64_t") {
        return kULong64_t;
    }
    if (type_view == "Short_t") {
        return kShort_t;
    }
    if (type_view == "UShort_t") {
        return kUShort_t;
    }
    if (type_view == "Char_t") {
        return kChar_t;
    }
    if (type_view == "UChar_t") {
        return kUChar_t;
    }
    if (type_view == "Bool_t") {
        return kBool_t;
    }
    return kNoType_t;
#endif
}

bool isSupportedLeaf(EDataType type) {
    switch (type) {
    case kFloat_t:
    case kFloat16_t:
#ifdef kFloat16Alt_t
    case kFloat16Alt_t:
#endif
    case kDouble_t:
    case kDouble32_t:
    case kInt_t:
    case kUInt_t:
    case kLong64_t:
    case kULong64_t:
    case kShort_t:
    case kUShort_t:
    case kChar_t:
    case kUChar_t:
    case kBool_t:
        return true;
    default:
        return false;
    }
}

ColumnSpec::ValueType inferValueType(EDataType type) {
    switch (type) {
    case kFloat_t:
    case kFloat16_t:
#ifdef kFloat16Alt_t
    case kFloat16Alt_t:
#endif
        return ColumnSpec::ValueType::Float;
    default:
        return ColumnSpec::ValueType::Double;
    }
}

std::vector<ColumnSpec> buildColumnSpecs(TTree *tree, const std::vector<ColumnOverride> &overrides) {
    if (!tree) {
        throw std::runtime_error("Score file does not contain the requested tree");
    }

    std::vector<ColumnSpec> specs;
    specs.reserve(overrides.empty() ? static_cast<std::size_t>(tree->GetListOfBranches()->GetEntries()) : overrides.size());

    if (!overrides.empty()) {
        std::unordered_set<std::string> seen_outputs;
        for (const auto &override_entry : overrides) {
            const auto &input = override_entry.first;
            const auto &output = override_entry.second;
            if (input == "event_uid") {
                throw std::runtime_error("event_uid is attached automatically and must not be listed in --columns");
            }
            auto *branch = tree->GetBranch(input.c_str());
            if (!branch) {
                throw std::runtime_error("Score column '" + input + "' was not found in " + std::string{tree->GetName()});
            }
        auto *leaf = branch->GetLeaf(input.c_str());
        if (!leaf) {
            throw std::runtime_error("Score column '" + input + "' is missing a leaf definition");
        }
        if (leaf->GetLen() > 1 || leaf->GetLeafCount() != nullptr) {
            throw std::runtime_error("Score column '" + input + "' is not a flat scalar branch");
        }
        const auto type = getLeafType(leaf);
        if (!isSupportedLeaf(type)) {
            throw std::runtime_error("Score column '" + input + "' uses an unsupported data type");
        }
            if (!seen_outputs.insert(output).second) {
                throw std::runtime_error("Duplicate output column name '" + output + "' requested");
            }
            ColumnSpec spec;
            spec.input_name = input;
            spec.output_name = output;
            spec.leaf_type = type;
            spec.value_type = inferValueType(type);
            specs.push_back(std::move(spec));
        }
        return specs;
    }

    auto *branches = tree->GetListOfBranches();
    std::unordered_set<std::string> seen_outputs;
    for (int idx = 0; idx < branches->GetEntries(); ++idx) {
        auto *branch = dynamic_cast<TBranch *>(branches->At(idx));
        if (!branch) {
            continue;
        }
        const std::string name = branch->GetName();
        if (name == "event_uid") {
            continue;
        }
        auto *leaf = branch->GetLeaf(name.c_str());
        if (!leaf) {
            continue;
        }
        const auto type = getLeafType(leaf);
        if (!isSupportedLeaf(type)) {
            continue;
        }
        if (leaf->GetLen() > 1 || leaf->GetLeafCount() != nullptr) {
            continue;
        }
        if (!seen_outputs.insert(name).second) {
            continue;
        }
        ColumnSpec spec;
        spec.input_name = name;
        spec.output_name = name;
        spec.leaf_type = type;
        spec.value_type = inferValueType(type);
        specs.push_back(std::move(spec));
    }

    return specs;
}

struct EventReaderBase {
    virtual ~EventReaderBase() = default;
    virtual ULong64_t value() = 0;
};

template <typename T>
struct EventReader : EventReaderBase {
    explicit EventReader(TTreeReader &reader) : value_reader(reader, "event_uid") {}
    ULong64_t value() override { return static_cast<ULong64_t>(*value_reader); }
    TTreeReaderValue<T> value_reader;
};

std::unique_ptr<EventReaderBase> buildEventReader(TTreeReader &reader, EDataType type) {
    switch (type) {
    case kULong64_t:
        return std::make_unique<EventReader<ULong64_t>>(reader);
    case kLong64_t:
        return std::make_unique<EventReader<Long64_t>>(reader);
    case kUInt_t:
        return std::make_unique<EventReader<UInt_t>>(reader);
    case kInt_t:
        return std::make_unique<EventReader<Int_t>>(reader);
    default:
        throw std::runtime_error("Unsupported type for event_uid branch");
    }
}

struct ColumnReaderBase {
    virtual ~ColumnReaderBase() = default;
    virtual double value() = 0;
};

template <typename T>
struct ColumnReader : ColumnReaderBase {
    ColumnReader(TTreeReader &reader, const std::string &column) : value_reader(reader, column.c_str()) {}
    double value() override { return static_cast<double>(*value_reader); }
    TTreeReaderValue<T> value_reader;
};

std::unique_ptr<ColumnReaderBase> buildColumnReader(TTreeReader &reader, const ColumnSpec &spec) {
    switch (spec.leaf_type) {
    case kFloat_t:
    case kFloat16_t:
#ifdef kFloat16Alt_t
    case kFloat16Alt_t:
#endif
        return std::make_unique<ColumnReader<Float_t>>(reader, spec.input_name);
    case kDouble_t:
    case kDouble32_t:
        return std::make_unique<ColumnReader<Double_t>>(reader, spec.input_name);
    case kInt_t:
        return std::make_unique<ColumnReader<Int_t>>(reader, spec.input_name);
    case kUInt_t:
        return std::make_unique<ColumnReader<UInt_t>>(reader, spec.input_name);
    case kLong64_t:
        return std::make_unique<ColumnReader<Long64_t>>(reader, spec.input_name);
    case kULong64_t:
        return std::make_unique<ColumnReader<ULong64_t>>(reader, spec.input_name);
    case kShort_t:
        return std::make_unique<ColumnReader<Short_t>>(reader, spec.input_name);
    case kUShort_t:
        return std::make_unique<ColumnReader<UShort_t>>(reader, spec.input_name);
    case kChar_t:
        return std::make_unique<ColumnReader<Char_t>>(reader, spec.input_name);
    case kUChar_t:
        return std::make_unique<ColumnReader<UChar_t>>(reader, spec.input_name);
    case kBool_t:
        return std::make_unique<ColumnReader<Bool_t>>(reader, spec.input_name);
    default:
        throw std::runtime_error("Unsupported score column type encountered");
    }
}

ScoreTable loadScoreTable(const std::string &file_path, const std::string &tree_name,
                          const std::vector<ColumnOverride> &overrides) {
    TFile score_file(file_path.c_str(), "READ");
    if (score_file.IsZombie()) {
        throw std::runtime_error("Failed to open score file: " + file_path);
    }

    auto *tree = dynamic_cast<TTree *>(score_file.Get(tree_name.c_str()));
    if (!tree) {
        throw std::runtime_error("Score tree '" + tree_name + "' was not found in " + file_path);
    }

    auto *uid_leaf = tree->GetLeaf("event_uid");
    if (!uid_leaf) {
        throw std::runtime_error("Score tree is missing the required event_uid branch");
    }

    auto column_specs = buildColumnSpecs(tree, overrides);
    if (column_specs.empty()) {
        throw std::runtime_error("No score columns were selected for attachment");
    }

    TTreeReader reader(tree);
    auto event_reader = buildEventReader(reader, getLeafType(uid_leaf));

    std::vector<std::unique_ptr<ColumnReaderBase>> column_readers;
    column_readers.reserve(column_specs.size());
    for (const auto &spec : column_specs) {
        column_readers.push_back(buildColumnReader(reader, spec));
    }

    ScoreTable table;
    table.columns = column_specs;
    const auto expected = tree->GetEntries();
    if (expected > 0) {
        table.values.reserve(static_cast<std::size_t>(expected));
    }

    while (reader.Next()) {
        const auto uid = event_reader->value();
        std::vector<double> row;
        row.reserve(column_readers.size());
        for (const auto &col_reader : column_readers) {
            row.push_back(col_reader->value());
        }

        auto [it, inserted] = table.values.emplace(uid, std::vector<double>{});
        if (!inserted) {
            ++table.duplicate_uids;
        }
        it->second = std::move(row);
        ++table.total_rows;
    }

    return table;
}

std::string buildSamplePrefix(const proc::HubDataFrame::CatalogEntry &entry) {
    std::vector<std::string> components;
    components.push_back(sanitiseComponent(entry.sample_key.empty() ? std::string{"sample"} : entry.sample_key));
    if (!entry.beam.empty()) {
        components.push_back(sanitiseComponent(entry.beam));
    }
    if (!entry.period.empty()) {
        components.push_back(sanitiseComponent(entry.period));
    }
    if (!entry.stage.empty()) {
        components.push_back(sanitiseComponent(entry.stage));
    }
    if (!entry.origin.empty()) {
        components.push_back(sanitiseComponent(entry.origin));
    }

    std::string result;
    bool first = true;
    for (const auto &component : components) {
        if (component.empty()) {
            continue;
        }
        if (!first) {
            result.push_back('_');
        }
        result.append(component);
        first = false;
    }

    if (result.empty()) {
        return std::string{"entry"};
    }
    return result;
}

std::string buildVariationTag(const proc::HubDataFrame::CatalogEntry &entry, const std::string &label) {
    std::string variation = entry.variation.empty() ? std::string{"nominal"} : entry.variation;
    variation = sanitiseComponent(variation);
    if (!variation.empty()) {
        variation.push_back('_');
    }
    variation.append(label);
    return variation;
}

std::filesystem::path makeRelativeToHub(const std::filesystem::path &path,
                                        const std::filesystem::path &hub_dir) {
    std::error_code ec;
    auto relative = std::filesystem::relative(path, hub_dir, ec);
    if (!ec) {
        return relative;
    }
    return path;
}

void attachScores(const Options &opts) {
    if (opts.hub_path.empty() || opts.scores_path.empty() || opts.scores_tree.empty() || opts.label.empty()) {
        throw std::runtime_error("--hub, --scores, --tree, and --label are required arguments");
    }

    const std::filesystem::path hub_path = std::filesystem::absolute(opts.hub_path);
    const std::filesystem::path hub_dir = hub_path.parent_path();

    const std::string friend_label = sanitiseComponent(opts.label);
    std::string friend_tree_name = opts.friend_tree.empty() ? friend_label : sanitiseComponent(opts.friend_tree);
    if (friend_label != opts.label) {
        proc::log::info("hub-attach-friends", "Friend label normalised to", friend_label);
    }
    if (!opts.friend_tree.empty() && friend_tree_name != opts.friend_tree) {
        proc::log::info("hub-attach-friends", "Friend tree name normalised to", friend_tree_name);
    }

    std::filesystem::path output_dir = opts.output_dir;
    if (output_dir.empty()) {
        output_dir = hub_dir / "friends" / friend_label;
    } else if (!output_dir.is_absolute()) {
        output_dir = hub_dir / output_dir;
    }

    proc::log::info("hub-attach-friends", "Loading score table from", opts.scores_path, "tree", opts.scores_tree);
    auto score_table = loadScoreTable(opts.scores_path, opts.scores_tree, opts.column_overrides);
    proc::log::info("hub-attach-friends", "Loaded", score_table.total_rows, "score rows covering", score_table.values.size(),
                    "unique events");
    if (score_table.duplicate_uids > 0) {
        proc::log::info("hub-attach-friends", "[warning]", score_table.duplicate_uids,
                        "duplicate event_uid entries were replaced by the most recent values");
    }

    if (score_table.values.empty()) {
        throw std::runtime_error("Score table is empty; nothing to attach");
    }

    proc::HubDataFrame hub(opts.hub_path);

    std::vector<std::string> friend_columns;
    friend_columns.reserve(score_table.columns.size() + 1);
    friend_columns.push_back("event_uid");
    for (const auto &column : score_table.columns) {
        friend_columns.push_back(column.output_name);
    }

    auto lookup = std::make_shared<const std::unordered_map<ULong64_t, std::vector<double>>>(std::move(score_table.values));

    std::vector<proc::HubFriend> new_friend_entries;
    new_friend_entries.reserve(hub.catalog().size());

    const auto &entries = hub.catalog();
    std::size_t updated_entries = 0;

    for (const auto &entry : entries) {
        if (entry.n_events == 0ULL) {
            continue;
        }

        auto existing_friend = std::find_if(entry.friends.begin(), entry.friends.end(),
                                            [&](const proc::HubDataFrame::CatalogEntry::FriendInfo &info) {
                                                return info.label == friend_label;
                                            });

        std::string tree_name = friend_tree_name;
        bool needs_metadata = true;
        std::filesystem::path existing_path;
        if (existing_friend != entry.friends.end()) {
            tree_name = existing_friend->tree.empty() ? friend_tree_name : existing_friend->tree;
            if (!existing_friend->path.empty()) {
                existing_path = existing_friend->path;
                if (!existing_path.is_absolute()) {
                    existing_path = hub_dir / existing_path;
                }
                needs_metadata = false;
            }
        }

        proc::HubDataFrame::Selection selection = hub.select();
        selection.sample(entry.sample_key)
            .beam(entry.beam)
            .period(entry.period)
            .variation(entry.variation)
            .origin(entry.origin)
            .stage(entry.stage);
        auto df = selection.load();

        ROOT::RDF::RNode node = df;
        for (std::size_t idx = 0; idx < score_table.columns.size(); ++idx) {
            const auto column = score_table.columns[idx];
            if (column.value_type == ColumnSpec::ValueType::Float) {
                const float missing = std::numeric_limits<float>::quiet_NaN();
                node = node.Define(column.output_name,
                                   [lookup, idx, missing](ULong64_t uid) -> float {
                                       const auto it = lookup->find(uid);
                                       if (it == lookup->end()) {
                                           return missing;
                                       }
                                       const auto &row = it->second;
                                       if (idx >= row.size()) {
                                           return missing;
                                       }
                                       return static_cast<float>(row[idx]);
                                   },
                                   {"event_uid"});
            } else {
                const double missing = std::numeric_limits<double>::quiet_NaN();
                node = node.Define(column.output_name,
                                   [lookup, idx, missing](ULong64_t uid) -> double {
                                       const auto it = lookup->find(uid);
                                       if (it == lookup->end()) {
                                           return missing;
                                       }
                                       const auto &row = it->second;
                                       if (idx >= row.size()) {
                                           return missing;
                                       }
                                       return row[idx];
                                   },
                                   {"event_uid"});
            }
        }

        proc::FriendWriter::FriendConfig config;
        config.output_dir = output_dir;
        config.tree_name = tree_name;
        proc::FriendWriter writer(config);

        std::filesystem::path written_path;
        if (!existing_path.empty()) {
            written_path = writer.writeFriendToPath(node, existing_path, friend_columns);
        } else {
            const auto sample_prefix = buildSamplePrefix(entry);
            const auto variation_tag = buildVariationTag(entry, friend_label);
            written_path = writer.writeFriend(node, sample_prefix, variation_tag, friend_columns);
        }

        proc::log::info("hub-attach-friends", "Attached", friend_label, "for", entry.sample_key, entry.variation,
                        "->", written_path.string());
        ++updated_entries;

        if (needs_metadata) {
            proc::HubFriend friend_entry;
            friend_entry.entry_id = entry.entry_id;
            friend_entry.label = friend_label;
            friend_entry.tree = tree_name;
            friend_entry.path = makeRelativeToHub(written_path, hub_dir).generic_string();
            new_friend_entries.push_back(std::move(friend_entry));
        }
    }

    if (!new_friend_entries.empty()) {
        proc::log::info("hub-attach-friends", "Registering", new_friend_entries.size(),
                        "new friend metadata entries");
        proc::HubCatalog catalog(opts.hub_path, proc::HubCatalog::OpenMode::Update);
        catalog.addFriends(new_friend_entries);
    }

    proc::log::info("hub-attach-friends", "Updated", updated_entries, "hub entries with", friend_label, "scores");
}

} // namespace

int main(int argc, char **argv) {
    try {
        auto options = parseOptions(argc, argv);
        if (options.show_help) {
            printUsage();
            return 0;
        }
        attachScores(options);
        return 0;
    } catch (const std::exception &ex) {
        std::cerr << "hub-attach-friends: " << ex.what() << std::endl;
        return 1;
    }
}
