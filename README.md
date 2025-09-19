# rarexsec-ntuples

`rarexsec-ntuples` provides a lightweight C++17 library and CLI tools for
assembling ROOT `RDataFrame` pipelines that are driven by JSON catalogues of
beam periods and samples.

## Requirements

- GCC 9+/Clang 10+ (or any C++17-capable toolchain)
- [CMake](https://cmake.org/) 3.16 or newer
- [ROOT](https://root.cern/) with the `RIO`, `Tree`, `Hist`, and `ROOTDataFrame`
  components on your `CMAKE_PREFIX_PATH`
- The header-only [nlohmann/json](https://github.com/nlohmann/json) dependency
  is fetched automatically during configuration

## Build

### Makefile shortcuts

```bash
make             # configure into ./build and compile the library + runners
make install     # install into ./install by default
make test        # run ctest (a placeholder target is provided)
make distclean   # remove the ./build directory entirely
```

### Manual CMake invocation

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
cmake --install build --prefix <path>  # optional custom prefix
```

The default installation prefix is `./install`. To install into a system prefix
such as `/usr/local`, either pass `-DCMAKE_INSTALL_PREFIX=/usr/local` when
configuring or set `-DRAREXSEC_ALLOW_SYSTEM_INSTALL=ON`.

## Generate a sample catalogue

The helper script converts an analysis recipe into the catalogue format expected
by the runners and copies/links the required ntuples:

```bash
python config/tools/build-catalogue.py --recipe config/analysis-recipe.json
```

The resulting JSON is written to `config/catalogues/samples.json` by default and
includes the resolved dataset identifiers, POT totals, trigger counts, and
relative file paths.

## Run the command-line tools

### Event processing runner

```bash
./build/main/rarexsec-runner/rarexsec-runner \
    config/catalogues/samples.json \
    numi \
    run1,run2 \
    [optional-selection] \
    [optional-output.root]
```

- Arguments 1–3 select the catalogue, beam, and comma-separated run periods.
- Provide a filter expression to `optional-selection` to trim events with
  `RDataFrame::Filter` syntax.
- Supply an output file to snapshot the selected events. When omitted the tool
  prints the available branches for each configured sample.

### Training pool generator

```bash
./build/main/rarexsec-training-pool-generator/rarexsec-training-pool-generator \
    config/catalogues/samples.json \
    numi \
    run1,run2 \
    [optional-selection] \
    output.root
```

The training generator records the event identifiers, weights, truth-channel
labels, and CNN-friendly image tensors while retaining the same snapshot layout
as the main runner.

## Snapshot layout

Snapshots follow a consistent directory structure inside the output ROOT file:

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

- `meta/totals` stores the integrated POT and trigger counts across all samples.
- `meta/samples` lists each nominal and detector-variation dataset together with
  the resolved `tree_path`, dataset identifier, beam, and run period.

## Explore snapshot metadata with ROOT

The snippet below iterates over the metadata summary, loads each nominal event
TTree, and prints the entry counts. Save it as `rarexsec_snapshot_example.C` and
run `root -l rarexsec_snapshot_example.C` to execute the helper:

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
            continue;
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

Use the same pattern to access detector variations (`variation` branch) or to
aggregate POT across selected subsets.
