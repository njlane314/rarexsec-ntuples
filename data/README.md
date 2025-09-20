# Configuration Data

This directory stores JSON assets describing datasets and run configurations
consumed by the helper utilities and C++ study programmes.

- `catalogues/` – JSON files describing datasets and run configuration.
  Helper tools read these catalogues to drive the runners.
- `recipe-template.json` – skeleton recipe to seed a new analysis
  configuration.
- `analysis-recipe.json` – fully populated example recipe generated from the
  template.

Companion utilities now live under `../tools/`. The scripts expect catalogue
files to reside beneath `data/catalogues/`, so adding a new configuration here
makes it immediately discoverable.
