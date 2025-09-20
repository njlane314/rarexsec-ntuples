# Configuration Layout

This directory contains configuration assets and helper tools.

- `data/` – JSON assets and other static inputs used by the processing tools
  and runners. It contains the example recipes together with generated
  catalogue JSON files.
  - `data/catalogues/` – JSON files describing datasets and run configuration.
    These catalogues are consumed by the helper tools and C++ study
    programmes.
  - `data/recipe-template.json` – skeleton recipe to seed a new analysis
    configuration.
  - `data/analysis-recipe.json` – fully populated example recipe generated from
    the template.
- `tools/` – Python modules and shell utilities that operate on the
  configuration data. For example, `tools/build-catalogue.py` reads an analysis
  recipe and writes out a catalogue JSON under `data/catalogues/`.

Tools reference files via paths relative to this directory, so placing new
configuration files under `data/` automatically makes them available to the
modules in `tools/`.
