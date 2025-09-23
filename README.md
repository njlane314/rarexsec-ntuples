# rarexsec-ntuples

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DRAREXSEC_BUILD_APPS=ON -DRAREXSEC_BUILD_LIB=ON
cmake --build build
cmake --install build
```

## Generate catalogues

```bash
python tools/build-catalogue.py --recipe data/analysis-recipe.json
ls data/catalogues
```

## Run the hub generator

```bash
mkdir -p outputs
# pass '' to accept the default selection; replace it to filter events
build/app/snapshot-analysis \
  data/catalogues/samples.json \
  numi-fhc \
  run1-run3 \
  '' \
  outputs/analysis_fhc_run1-3.hub.root
root -l -q 'hub_preview.C("outputs/analysis_fhc_run1-3.hub.root")'
```

## Snapshot for model training

```bash
# pass '' to keep the built-in training selection or provide your own filter
build/app/snapshot-training \
  data/catalogues/samples.json \
  numi-fhc \
  run1-run3 \
  '' \
  outputs/training_fhc_run1-3.hub.root
```

## Attach CNN friend shards

```bash
mkdir -p /scratch/$USER/rarexsec/friends
hub-attach-friends \
  --hub /path/to/catalogue.hub.root \
  --scores /path/to/scores.root \
  --tree cnn_output \
  --label cnn \
  --output-dir /scratch/$USER/rarexsec/friends
ls /scratch/$USER/rarexsec/friends
```

## Sync shards to gpvm scratch

```bash
mkdir -p /scratch/$USER/rarexsec/shards
rsync -av friend_shards/ /scratch/$USER/rarexsec/shards/
```
