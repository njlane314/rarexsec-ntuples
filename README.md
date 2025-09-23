# rarexsec-ntuples

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DRAREXSEC_BUILD_APPS=ON -DRAREXSEC_BUILD_LIB=ON
cmake --build build
cmake --install build
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
