# rarexsec Processing Toolkit

This repository provides a small processing library and a command-line runner
for assembling ROOT `RDataFrame` pipelines driven by configuration JSON files.

## Build requirements

The project relies on the following external dependencies:

- A C++17 toolchain (GCC 9+, Clang 10+, or compatible)
- [CMake](https://cmake.org/) 3.16 or newer
- [ROOT](https://root.cern/) with the `RIO`, `Tree`, `Hist`, and `ROOTDataFrame`
  components available on your `CMAKE_PREFIX_PATH`

The header-only [nlohmann/json](https://github.com/nlohmann/json) dependency is
fetched automatically at configure time via CMake's `FetchContent` module.

## Building

You can drive the build with the provided Makefile, which wraps the standard
CMake workflow:

```bash
make                # configure (in ./build) and compile the library and runner
make test           # invoke ctest (no tests are defined yet, but the target exists)
make install        # install into ./install (or override via CMAKE_INSTALL_PREFIX)
make distclean      # remove the build directory completely
```

Alternatively you can call CMake directly:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

The default installation prefix is the repository-local `./install` directory.
This avoids write-permission issues on shared systems where `/usr/local` is
read-only. Override it either when configuring
(`-DCMAKE_INSTALL_PREFIX=/path/to/prefix`) or at install time
(`make install INSTALL_FLAGS="--prefix=/path/to/prefix"`). To explicitly keep a
system prefix such as `/usr/local`, configure with
`-DRAREXSEC_ALLOW_SYSTEM_INSTALL=ON`.

## Running the processing CLI

The command-line entry point now lives under `main/`. After
building, the executable is placed under `build/main/rarexsec-runner/` by default. Invoke it with the configuration JSON, beam, and periods. The runner
infers the ntuple base directory from the configuration file (honouring either
`ntuple_base_directory` or `samples.ntupledir`), and optional trailing arguments
can be used to select events or snapshot the output:

```bash
./build/main/rarexsec-runner/rarexsec-runner \
    config/catalogs/samples.json \
    numi \
    run1,run2 \
    [optional-selection] \
    [optional-output.root]
```

If an output file is provided the selected events are snapshotted into that ROOT
file; otherwise the available branches for the configured samples are printed to
standard output.

## Snapshot output layout

When an output file is requested the runner writes a structured ROOT file to
make interactive browsing and downstream analysis easier:

- `samples/<sample>/nominal/events` contains the nominal event tree for each
  configured sample.
- `samples/<sample>/variations/<variation>/events` holds detector systematics
  associated with that sample. Variation names are normalised so that any
  characters outside `[A-Za-z0-9_-]` are replaced with underscores.
- `meta/totals` captures the integrated POT and trigger counts across all
  processed samples.
- `meta/samples` summarises each nominal and variation entry with branches such
  as `sample_key`, `dataset_id`, and the resolved `tree_path` pointing to the
  location of the corresponding events tree.

The overall layout looks like:

```text
snapshot.root
├── samples/
│   └── <sample>/
│       ├── nominal/
│       │   └── events (TTree)
│       └── variations/
│           └── <variation>/
│               └── events (TTree)
└── meta/
    ├── totals (TTree)
    └── samples (TTree)
```

The `<sample>` directory component is derived from the sample key in the recipe
after applying the same character normalisation described above, so keys such as
`mc_inclusive_run1_fhc` stay readable while more elaborate identifiers are still
legal ROOT directory names.

To explore the output interactively you can drop the following helper macro into
`rarexsec_snapshot_example.C` and run it from a ROOT session. It loops over the
metadata tree, grabs each nominal tree, and prints its entry count:

```cpp
#include <iostream>
#include <string>

#include "TFile.h"
#include "TTree.h"

void rarexsec_snapshot_example(const char *filename = "snapshot.root") {
    TFile input(filename, "READ");
    if (input.IsZombie()) {
        std::cerr << "Could not open " << filename << "\n";
        return;
    }

    auto *meta = static_cast<TTree *>(input.Get("meta/samples"));
    if (!meta) {
        std::cerr << "meta/samples tree is missing\n";
        return;
    }

    std::string tree_path;
    std::string sample_key;
    meta->SetBranchAddress("tree_path", &tree_path);
    meta->SetBranchAddress("sample_key", &sample_key);

    for (Long64_t entry = 0; entry < meta->GetEntries(); ++entry) {
        meta->GetEntry(entry);
        if (tree_path.find("/nominal/") == std::string::npos) {
            continue; // only inspect the nominal samples in this example
        }

        auto *events = static_cast<TTree *>(input.Get(tree_path.c_str()));
        if (!events) {
            std::cerr << "Tree not found: " << tree_path << "\n";
            continue;
        }
        std::cout << sample_key << " -> " << tree_path << " has "
                  << events->GetEntries() << " entries\n";
    }
}
```

The macro demonstrates how to rely on the metadata summary rather than hard
coding paths; more elaborate scripts can branch on `variation` or `origin` to
automate detector variation studies.

