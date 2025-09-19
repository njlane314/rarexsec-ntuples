# Configuration Layout

This directory contains configuration assets and helper tools.

- `catalogues/` – JSON files describing datasets and run configuration. These
  catalogues are consumed by the helper tools and C++ study programmes.
- `tools/` – Python modules and shell utilities that operate on the
  configuration data. For example, `tools/build-catalogue.py` reads an analysis
  recipe and writes out a catalogue JSON.
- `recipe_template.json` – skeleton recipe to seed a new analysis
  configuration.
- `analysis_recipe.json` – fully populated example recipe generated from the
  template.

Tools reference files via paths relative to this directory, so placing a
configuration file under `catalogues/` automatically makes it available to the
modules in `tools/`.
