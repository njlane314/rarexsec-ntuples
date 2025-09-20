// Lightweight ROOT macro that prints the structure of a ROOT file.
//
// Usage from the ROOT prompt:
//   root [0] .L app/print_root_structure.C
//   root [1] print_root_structure("path/to/file.root")
//
// The macro lists the hierarchy of directories, objects and TTrees.
// For TTrees it also prints their branches and the leaf types.

#include "TBranch.h"
#include "TDirectory.h"
#include "TFile.h"
#include "TKey.h"
#include "TLeaf.h"
#include "TObjArray.h"
#include "TTree.h"

#include <cstddef>
#include <iostream>
#include <string>

namespace {

std::string indentation(int depth) {
    return std::string(static_cast<std::size_t>(depth) * 2, ' ');
}

void printTreeBranches(TTree *tree, int depth) {
    if (!tree) {
        return;
    }

    const std::string branch_indent = indentation(depth);
    TObjArray *branches = tree->GetListOfBranches();
    if (!branches) {
        return;
    }

    for (int i = 0; i < branches->GetEntriesFast(); ++i) {
        auto *branch = dynamic_cast<TBranch *>(branches->At(i));
        if (!branch) {
            continue;
        }

        std::cout << branch_indent << "- " << branch->GetName();

        const auto *leaves = branch->GetListOfLeaves();
        if (leaves && leaves->GetEntriesFast() > 0) {
            std::cout << " [";
            bool first = true;
            for (int j = 0; j < leaves->GetEntriesFast(); ++j) {
                auto *leaf = dynamic_cast<TLeaf *>(leaves->At(j));
                if (!leaf) {
                    continue;
                }
                if (!first) {
                    std::cout << ", ";
                }
                std::cout << leaf->GetName();
                const char *type_name = leaf->GetTypeName();
                if (type_name && type_name[0] != '\0') {
                    std::cout << ":" << type_name;
                }
                first = false;
            }
            std::cout << "]";
        } else {
            const char *title = branch->GetTitle();
            if (title && title[0] != '\0') {
                std::cout << " [" << title << "]";
            }
        }

        std::cout << '\n';
    }
}

void printDirectory(TDirectory *directory, int depth) {
    if (!directory) {
        return;
    }

    TIter next_key(directory->GetListOfKeys());
    while (TKey *key = static_cast<TKey *>(next_key())) {
        const std::string current_indent = indentation(depth);
        std::cout << current_indent << key->GetName() << " (" << key->GetClassName() << ")\n";

        TObject *object = key->ReadObj();
        if (!object) {
            continue;
        }

        if (object->InheritsFrom(TDirectory::Class())) {
            printDirectory(static_cast<TDirectory *>(object), depth + 1);
        } else if (object->InheritsFrom(TTree::Class())) {
            printTreeBranches(static_cast<TTree *>(object), depth + 1);
        }

        delete object;
    }
}

} // namespace

void print_root_structure(const char *file_name) {
    if (!file_name || file_name[0] == '\0') {
        std::cerr << "No file name provided to print_root_structure." << std::endl;
        return;
    }

    TFile file(file_name, "READ");
    if (!file.IsOpen() || file.IsZombie()) {
        std::cerr << "Failed to open ROOT file: " << file_name << std::endl;
        return;
    }

    std::cout << "Contents of " << file_name << "\n";
    printDirectory(&file, 0);

    file.Close();
}
