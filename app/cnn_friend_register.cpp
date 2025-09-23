#include <rarexsec/HubFriendLinker.h>
#include <rarexsec/LoggerUtils.h>

#include <exception>
#include <iostream>
#include <string>

namespace {

void printUsage(std::ostream &os) {
    os << "Usage: cnn-friend-register <hub.hub.root> [options]\n"
       << "\n"
       << "Options:\n"
       << "  --single-file <path>     Attach the same friend file to every hub entry\n"
       << "  --shard-dir <directory>  Directory containing per-shard friend files\n"
       << "  --keep-structure         Preserve shard subdirectories under --shard-dir\n"
       << "  --suffix <text>          Append text to the shard stem when building friend filenames\n"
       << "  --extension <ext>        Replace the shard file extension (with or without the leading dot)\n"
       << "  --tree <name>            Record the friend tree name in the hub catalogue\n"
       << "  --allow-missing          Skip shards without a friend file instead of aborting\n"
       << "  --absolute-paths         Store absolute friend paths (default: relative to the hub)\n"
       << "  --help                   Show this message\n";
}

} // namespace

int main(int argc, char **argv) {
    if (argc < 2) {
        printUsage(std::cerr);
        return 1;
    }

    if (std::string(argv[1]) == "--help" || std::string(argv[1]) == "-h") {
        printUsage(std::cout);
        return 0;
    }

    proc::FriendLinkOptions options;
    options.hub_path = argv[1];

    bool has_single = false;
    bool has_dir = false;

    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--single-file") {
            if (has_dir) {
                std::cerr << "--single-file conflicts with --shard-dir" << std::endl;
                return 1;
            }
            if (i + 1 >= argc) {
                std::cerr << "--single-file requires a path argument" << std::endl;
                return 1;
            }
            options.friend_file = std::string(argv[++i]);
            has_single = true;
        } else if (arg == "--shard-dir") {
            if (has_single) {
                std::cerr << "--shard-dir conflicts with --single-file" << std::endl;
                return 1;
            }
            if (i + 1 >= argc) {
                std::cerr << "--shard-dir requires a directory argument" << std::endl;
                return 1;
            }
            options.friend_directory = std::string(argv[++i]);
            has_dir = true;
        } else if (arg == "--suffix") {
            if (i + 1 >= argc) {
                std::cerr << "--suffix requires a value" << std::endl;
                return 1;
            }
            options.filename_suffix = std::string(argv[++i]);
        } else if (arg == "--extension") {
            if (i + 1 >= argc) {
                std::cerr << "--extension requires a value" << std::endl;
                return 1;
            }
            options.filename_extension = std::string(argv[++i]);
        } else if (arg == "--tree") {
            if (i + 1 >= argc) {
                std::cerr << "--tree requires a name" << std::endl;
                return 1;
            }
            options.friend_tree = std::string(argv[++i]);
        } else if (arg == "--allow-missing") {
            options.allow_missing = true;
        } else if (arg == "--keep-structure") {
            options.mirror_structure = true;
        } else if (arg == "--absolute-paths") {
            options.make_relative = false;
        } else if (arg == "--help" || arg == "-h") {
            printUsage(std::cout);
            return 0;
        } else {
            std::cerr << "Unknown option: " << arg << std::endl;
            printUsage(std::cerr);
            return 1;
        }
    }

    if (!has_single && !has_dir) {
        std::cerr << "Specify either --single-file or --shard-dir" << std::endl;
        return 1;
    }

    try {
        proc::linkFriendFiles(options);
    } catch (const std::exception &ex) {
        proc::log::info("cnn-friend-register", "[error]", ex.what());
        std::cerr << "cnn-friend-register: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
