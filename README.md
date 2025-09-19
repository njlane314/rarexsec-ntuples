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
make install        # install into CMAKE_INSTALL_PREFIX (defaults to /usr/local)
make distclean      # remove the build directory completely
```

Alternatively you can call CMake directly:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Running the processing CLI

The command-line entry point now lives under `apps/rarexsec-runner/`. After
building, the executable is placed under `build/apps/rarexsec-runner/` by
default. Invoke it with the configuration JSON, beam, and periods. The runner
infers the ntuple base directory from the configuration file (honouring either
`ntuple_base_directory` or `samples.ntupledir`), and optional trailing arguments
can be used to select events or snapshot the output:

```bash
./build/apps/rarexsec-runner/rarexsec-runner \
    config/catalogs/samples.json \
    numi \
    run1,run2 \
    [optional-selection] \
    [optional-output.root]
```

If an output file is provided the selected events are snapshotted into that ROOT
file; otherwise the available branches for the configured samples are printed to
standard output.

