# Configuration Layout

This directory contains configuration assets and helper scripts.

- `catalogs/` – JSON files describing datasets and run configuration. These
  catalogs are consumed by the helper scripts and C++ study programs.
- `scripts/` – Python modules that operate on the configuration data. For
  example, `build_sample_catalog.py` reads an analysis recipe and writes out a
  catalog JSON.
- `recipe_template.json` – skeleton recipe to seed a new analysis
  configuration.
- `analysis_recipe.json` – fully populated example recipe generated from the
  template.

Scripts reference files via paths relative to this directory, so placing a
configuration file under `catalogs/` automatically makes it available to the
modules in `scripts/`.
