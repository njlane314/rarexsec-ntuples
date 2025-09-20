# rarexsec-ntuples

## Build

### Makefile shortcuts

```bash
make                # configure into ./build and compile the library + tools
make install        # install into ./install by default
make test           # run ctest (a placeholder target is provided)
make distclean      # remove the ./build directory entirely

make build-lib      # configure ./build-lib with only the processing library
make install-lib    # install artifacts from ./build-lib
make distclean-lib  # remove the ./build-lib directory

make build-apps     # configure ./build-apps with the library + executables
make install-apps   # install artifacts from ./build-apps
make distclean-apps # remove the ./build-apps directory
```

### Manual CMake invocation

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
cmake --install build --prefix <path>  # optional custom prefix
```

Component-specific toggles allow separate build directories when focusing on
library or executable development:

```bash
# Library only
cmake -S . -B build-lib -DRAREXSEC_BUILD_APPS=OFF

# Executables together with the in-tree library (default behaviour)
cmake -S . -B build-apps -DRAREXSEC_BUILD_LIB=ON -DRAREXSEC_BUILD_APPS=ON
```

`RAREXSEC_BUILD_APPS` requires the processing library because the tools link
to `rarexsec::processing`.

The default installation prefix is `./install`. To install into a system prefix
such as `/usr/local`, either pass `-DCMAKE_INSTALL_PREFIX=/usr/local` when
configuring or set `-DRAREXSEC_ALLOW_SYSTEM_INSTALL=ON`.

## Generate a sample catalogue

The helper script converts an analysis recipe into the catalogue format expected
by the runners and copies/links the required ntuples:

```bash
python tools/build-catalogue.py --recipe data/analysis-recipe.json
```

The resulting JSON is written to `data/catalogues/samples.json` by default and
includes the POT totals, trigger counts, and
relative file paths.

## Run the command-line tools

### Snapshot tool

```bash
./build/app/snapshot/snapshot \
    data/catalogues/samples.json \
    numi-fhc \
    run1-run3 \
    [optional-selection] \
    [optional-output.root]
```

- Arguments 1–3 select the catalogue, beam, and run periods. Beam modes follow
  the catalogue naming (for example `numi-fhc`, `numi-rhc`, or `bnb`). Periods
  accept comma-separated values like `run1,run2,run3`, numeric ranges such as
  `run1-run3`, or the special value `all` to process every configured run
  period. You can also append additional run tokens as separate arguments (for
  example `... run1 run2 run3 ...`) without quoting.
- Provide a filter expression to `optional-selection` to trim events with
  `RDataFrame::Filter` syntax.
- Supply an output file to snapshot the selected events. When omitted the tool
  prints the available branches for each configured sample.
- Include details such as the beam, period, and selection in the output file
  name to keep it self-descriptive; for example,
  `snapshot_fhc_r1-3_nuepre.root`.
- Snapshots include only the curated branch list defined in
  `requestedSnapshotColumns()` within `app/snapshot.cpp`.

### Training pool tool

```bash
./build/app/training-pool/training-pool \
    data/catalogues/samples.json \
    numi-fhc \
    all \
    [optional-selection] \
    output.root
```

The training pool command records the event identifiers, weights, truth-channel
labels, and CNN-friendly image tensors while retaining the same snapshot layout
as the snapshot tool. It honours the same period syntax as the snapshot runner,
so comma-separated lists (`run1,run2,run3`) or additional run tokens supplied as
separate arguments are all valid. Use a descriptive output name such as
`train_fhc_all_nuepre.root` so the resulting ROOT files can be quickly traced
back to their configuration.

## Snapshot layout

Snapshots follow a consistent directory structure inside the output ROOT file:

```text
snapshot.root
├── samples/
│   └── <beam-mode>/
│       └── <run-period>/
│           └── <origin>/
│               └── [<stage>/]
│                   └── <sample>/
│                       ├── nominal/
│                       │   └── events (TTree)
│                       └── variations/
│                           └── <variation>/
│                               └── events (TTree)
└── meta/
    ├── totals (TTree)
    └── samples (TTree)
```

- `beam-mode` and `run-period` ensure samples from different NuMI beam
  configurations never clash.
- The `origin` component reflects the catalogue entry (`mc`, `data`, `dirt`,
  `ext`, ...). A `stage` directory is only inserted when the sample or
  variation advertises a processing stage name.
- `meta/totals` stores the integrated POT and trigger counts across all samples.
- `meta/samples` lists each nominal and detector-variation dataset together with
  the resolved `tree_path`, beam, and run period.

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

## ROOT macro quickstart

```bash
root -l rarexsec_snapshot_example.C
root -l app/plot_example.C
root -l app/print_root_structure.C
```
