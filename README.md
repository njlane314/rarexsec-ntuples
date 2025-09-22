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

### Snapshot analysis tool

```bash
./build/app/snapshot-analysis/snapshot-analysis \
    data/catalogues/samples.json \
    numi-fhc \
    run1-run3 \
    [optional-selection] \
    [optional-output.hub.root]
```

- Arguments 1–3 select the catalogue, beam, and run periods. Beam modes follow
  the catalogue naming (for example `numi-fhc`, `numi-rhc`, or `bnb`). Periods
  accept comma-separated values like `run1,run2,run3`, numeric ranges such as
  `run1-run3`, or the special value `all` to process every configured run
  period. You can also append additional run tokens as separate arguments (for
  example `... run1 run2 run3 ...`) without quoting.
- Provide a filter expression to `optional-selection` to trim events with
  `RDataFrame::Filter` syntax.
- Supply an output hub file to snapshot the selected events. When omitted the
  tool prints the available branches for each configured sample. Hubs use the
  `.hub.root` suffix and create a sibling `shards/` directory that mirrors the
  input samples as shard files.
- Include details such as the beam, period, and selection in the output hub
  name to keep it self-descriptive; for example,
  `snapshot_fhc_r1-3_nuepre.hub.root`.
- Snapshots include only the curated branch list defined in
  `requestedSnapshotColumns()` within `app/snapshot_analysis.cpp`.
- ROOT's implicit multi-threading is enabled automatically with the maximum
  available threads.

### Snapshot training tool

```bash
./build/app/snapshot-training/snapshot-training \
    data/catalogues/samples.json \
    numi-fhc \
    all \
    [optional-selection] \
    output.hub.root
```

The training pool command records the event identifiers, weights, truth-channel
labels, and CNN-friendly image tensors while retaining the same hub layout as
the snapshot analysis tool. It honours the same period syntax as
`snapshot-analysis`, so comma-separated lists (`run1,run2,run3`) or additional
run tokens supplied as separate arguments are all valid.
- ROOT's implicit multi-threading is enabled automatically with the maximum
  available threads.

## Hub snapshots

Hubs (`*.hub.root`) are lightweight catalogues that describe every shard produced for a snapshot. The hub file stores the metadata while the shard payloads live in a sibling `shards/` directory. Each shard corresponds to a single sample/variation pair, carrying the requested columns for that dataset without further splitting.

### Catalogue contents

- `shards` (TTree) – one row per shard with identifier fields (`sample_id`, `beam_id`, `period_id`, `variation_id`, `origin_id`), the relative shard location (`shard_path`), per-shard event counts, weight sums, exposure totals, and human-readable keys.
- `hub_meta` (TTree) – key/value metadata entries. `provenance_dicts` preserves the dictionaries that map human-readable labels to the integer identifiers written into the shards, while `summary` stores the integrated POT and trigger totals for the complete hub.

On disk the hub catalogue and shards appear as follows:

```text
snapshot_fhc_r1-3_nuepre.hub.root
├── shards/
│   ├── nue_fhc_nominal_0000.root
│   └── nue_fhc_fluxup_0000.root
└── (hub catalog containing the shards and hub_meta TTrees)
```

### ROOT macro example

Save the snippet below as `hub_example.C` and execute `root -l hub_example.C` to analyse events straight from the hub catalogue:

```cpp
#include <rarexsec/HubDataFrame.h>

void hub_example() {
    proc::HubDataFrame hub{"snapshot_fhc_r1-3_nuepre.hub.root"};
    auto df = hub.query("numi-fhc", "run1", "nominal", "mc");

    auto hist = df.Filter("base_sel")
                  .Histo1D({"h_vtx_z", "Reconstructed vertex z", 120, 0., 600.},
                           "reco_neutrino_vertex_z");
    hist->Draw();
}
```

The `HubDataFrame` helper resolves the shard catalogue, builds the underlying `TChain`, and hands back an `RNode` that behaves exactly like the dataframe returned by the snapshot pipeline.

## ROOT macro quickstart

```bash
root -l hub_example.C
```
