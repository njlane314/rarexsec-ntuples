from __future__ import annotations
import argparse
import concurrent.futures
import json
import logging
import os
import sys
import sqlite3
from functools import lru_cache
from pathlib import Path
import xml.etree.ElementTree as ET
import re
import subprocess
import shutil
import uproot
from typing import Set, Tuple, List, Dict

DEFAULT_RUN_DB = "/exp/uboone/data/uboonebeam/beamdb/run.db"

HADD_TMPDIR = Path("/pnfs/uboone/scratch/users/nlane/tmp/")
MIN_FREE_GB = 5.0
DEFAULT_JOBS = min(8, os.cpu_count() or 1)
CATALOGUE_SUBDIR = Path("data") / "catalogues"

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

def norm_run(run: str) -> str:
    digits = "".join(ch for ch in run if ch.isdigit())
    return f"r{digits}" if digits else run

def split_beam_key(beam_key: str) -> tuple[str, str]:
    for sep in ("_", "-"):
        if sep in beam_key:
            b, m = beam_key.split(sep, 1)
            return b, m
    return beam_key, "data"

def runset_token(runs: list[str]) -> str:
    if not runs:
        return "all"
    if len(runs) == 1:
        return norm_run(runs[0])
    try:
        nums = sorted(int("".join(ch for ch in r if ch.isdigit())) for r in runs)
        if nums == list(range(nums[0], nums[-1] + 1)):
            return f"r{nums[0]}-r{nums[-1]}"
    except Exception:
        pass
    return ",".join(norm_run(r) for r in runs)

def summarise_beams_for_name(beam_keys: list[str]) -> str:
    beams = set(beam_keys)
    has_bnb = any(k.startswith("bnb") for k in beams)
    has_nu  = any(k.startswith("numi") for k in beams)
    if has_bnb and has_nu:
        return "nu+bnb"
    if has_bnb:
        return "bnb"
    if has_nu:
        pols = sorted({
            mode.lower()
            for k in beams
            for mode in [split_beam_key(k)[1]]
            if mode and mode.lower() not in {"data", "ext"}
        })
        pol_str = f"[{','.join(pols)}]" if pols else ""
        return f"nu{pol_str}"
    return "multi"

def get_xml_entities(xml_path: Path, content: str | None = None) -> dict[str, str]:
    if content is None:
        content = xml_path.read_text()
    entity_regex = re.compile(r"<!ENTITY\s+([^\s]+)\s+\"([^\"]+)\">")
    return {match.group(1): match.group(2) for match in entity_regex.finditer(content)}

def run_command(command: list[str], execute: bool) -> bool:
    print(f"[COMMAND] {' '.join(command)}")
    if not execute:
        print("[INFO] Dry run mode. HADD command not executed.")
        return True
    if shutil.which(command[0]) is None:
        print(
            f"[ERROR] Command '{command[0]}' not found. Ensure ROOT is set up by running:\n"
            "             source /cvmfs/larsoft.opensciencegrid.org/products/common/etc/setups\n"
            "             setup root",
            file=sys.stderr,
        )
        return False
    try:
        subprocess.run(command, check=True)
        print("[STATUS] HADD Execution successful.")
        return True
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] HADD Execution failed: {exc}", file=sys.stderr)
        return False

@lru_cache(maxsize=256)
def list_root_files(input_dir: str) -> tuple[str, ...]:
    return tuple(str(p) for p in Path(input_dir).rglob("*.root"))

_POT_CACHE: dict[tuple[str, float], float] = {}

def get_total_pot_from_single_file(file_path: Path) -> float:
    if not file_path or not file_path.is_file():
        return 0.0
    try:
        st = file_path.stat()
        key = (str(file_path), st.st_mtime)
        if key in _POT_CACHE:
            return _POT_CACHE[key]
        tree_path = f"{file_path}:nuselection/SubRun"
        with uproot.open(tree_path) as tree:
            arr = tree["pot"].array(library="np")
            pot = float(arr.sum())
        _POT_CACHE[key] = pot
        return pot
    except Exception as exc:
        print(f"    Warning: Could not read POT from {file_path}: {exc}", file=sys.stderr)
        return 0.0

def get_total_pot_from_files_parallel(file_paths: list[str], max_workers: int) -> float:
    if not file_paths:
        return 0.0
    paths = [Path(p) for p in file_paths]
    mw = min(len(paths), max_workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=mw) as ex:
        return sum(ex.map(get_total_pot_from_single_file, paths))

def resolve_input_dir(stage_name: str | None, stage_outdirs: dict) -> str | None:
    return stage_outdirs.get(stage_name) if stage_name else None

def pot_sum_via_iterate(input_dir: str) -> float:
    pot_sum = 0.0
    expr = f"{input_dir}/**/*.root:nuselection/SubRun"
    for chunk in uproot.iterate(expr, filter_name=["pot"], library="np", step_size="50 MB"):
        pot_sum += float(chunk["pot"].sum())
    return pot_sum

def _extract_pairs_from_file(f: Path) -> Set[Tuple[int, int]]:
    pairs: Set[Tuple[int, int]] = set()
    try:
        with uproot.open(f) as rf:
            candidates = [
                "nuselection/SubRun", "nuselection/SubRuns",
                "SubRun", "SubRuns",
                "subrun", "subruns",
                "nuselection/Events", "Events",
            ]

            def read_pairs(t) -> Set[Tuple[int, int]]:
                try:
                    # case-insensitive branch lookup
                    bmap = {k.lower(): k for k in t.keys()}
                    if "run" in bmap and "subrun" in bmap:
                        run = t[bmap["run"]].array(library="np")
                        sub = t[bmap["subrun"]].array(library="np")
                        return set(zip(run.astype(int).tolist(), sub.astype(int).tolist()))
                except Exception:
                    pass
                return set()

            # Try explicit common paths first
            for name in candidates:
                try:
                    t = rf[name]  # works for nested paths like "nuselection/SubRun"
                except Exception:
                    continue
                got = read_pairs(t)
                if got:
                    return got

            # Fallback: scan every TTree recursively
            try:
                for path, cls in (rf.classnames(recursive=True) or {}).items():
                    if cls == "TTree":
                        try:
                            got = read_pairs(rf[path])
                        except Exception:
                            continue
                        if got:
                            pairs |= got
                return pairs
            except Exception:
                return pairs
    except Exception as e:
        print(f"    Warning: {f}: failed extracting (run,subrun): {e}", file=sys.stderr)
    return pairs

def _collect_pairs_from_files(files: List[str]) -> Set[Tuple[int, int]]:
    pairs: Set[Tuple[int, int]] = set()
    for p in files:
        pairs |= _extract_pairs_from_file(Path(p))
    return pairs

def _sum_ext_triggers_from_pairs(run_db: str, pairs: Set[Tuple[int, int]]) -> tuple[int, List[Tuple[int, int]], Dict[int, int]]:
    if not pairs:
        return 0, [], {}
    conn = sqlite3.connect(run_db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("CREATE TEMP TABLE pairs(run INTEGER, subrun INTEGER);")
    cur.executemany("INSERT INTO pairs(run, subrun) VALUES (?, ?);", list(pairs))
    total = cur.execute("""
        SELECT IFNULL(SUM(r.EXTTrig), 0)
        FROM runinfo r
        JOIN pairs p ON r.run = p.run AND r.subrun = p.subrun;
    """).fetchone()[0]
    missing_rows = cur.execute("""
        SELECT p.run, p.subrun
        FROM pairs p
        LEFT JOIN runinfo r ON r.run = p.run AND r.subrun = p.subrun
        WHERE r.run IS NULL;
    """).fetchall()
    missing_pairs = [(int(x["run"]), int(x["subrun"])) for x in missing_rows]
    by_run_rows = cur.execute("""
        SELECT r.run AS run, IFNULL(SUM(r.EXTTrig), 0) AS ext_sum
        FROM runinfo r
        JOIN pairs p ON r.run = p.run AND r.subrun = p.subrun
        GROUP BY r.run
        ORDER BY r.run;
    """).fetchall()
    by_run = {int(r["run"]): int(r["ext_sum"]) for r in by_run_rows}
    conn.close()
    return int(total), missing_pairs, by_run

def process_sample_entry(
    entry: dict,
    processed_analysis_path: Path,
    stage_outdirs: dict,
    run_pot: float,
    ext_triggers: int,
    run_db: str,
    jobs: int,
    is_detvar: bool = False,
) -> bool:
    if not entry.get("active", True):
        sample_key = entry.get("sample_key", "UNKNOWN")
        print(f"  Skipping {'detector variation' if is_detvar else 'sample'}: {sample_key} (marked as inactive)")
        return False

    stage_name = entry.get("stage_name")
    sample_key = entry.get("sample_key")
    sample_type = entry.get("sample_type", "mc")

    print(f"  Processing {'detector variation' if is_detvar else 'sample'}: {sample_key} (from stage: {stage_name})")
    print(f"    HADD execution for this {'sample' if not is_detvar else 'detector variation'}: Enabled")

    input_dir = resolve_input_dir(stage_name, stage_outdirs)
    if not input_dir:
        print(
            f"    Warning: Stage '{stage_name}' not found in XML outdirs. Skipping {'detector variation' if is_detvar else 'sample'} '{sample_key}'.",
            file=sys.stderr,
        )
        return False

    output_file = processed_analysis_path / f"{sample_key}.root"
    output_dir = output_file.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    if not os.access(output_dir, os.W_OK):
        print(
            f"    Error: Output directory '{output_dir}' is not writable. "
            "Ensure it exists and you have write permission.",
            file=sys.stderr,
        )
        return False

    if output_file.exists():
        try:
            output_file.unlink()
        except OSError as exc:
            print(
                f"    Error: Cannot remove existing file '{output_file}': {exc}",
                file=sys.stderr,
            )
            return False

    entry["relative_path"] = output_file.name
    root_files = list(list_root_files(input_dir))

    if not root_files:
        print(
            f"    Warning: No ROOT files found in {input_dir}. HADD will be skipped.",
            file=sys.stderr,
        )
        print(
            f"    Note: No ROOT files found for '{sample_key}'. Skipping HADD but proceeding to record metadata (if applicable)."
        )
        source_files = []
    else:
        # Decide parallel vs single-process based on available space in fixed temp dir
        use_parallel = jobs > 1
        chosen_tmp = HADD_TMPDIR

        if use_parallel:
            try:
                chosen_tmp.mkdir(parents=True, exist_ok=True)
                free_gb = shutil.disk_usage(chosen_tmp).free / (1024 ** 3)
            except Exception as e:
                print(f"    Warning: Could not evaluate free space in '{chosen_tmp}': {e}. Falling back to single-process hadd.")
                free_gb = 0.0

            if free_gb < MIN_FREE_GB:
                print(f"    Note: Only {free_gb:.1f} GB free in '{chosen_tmp}'. Falling back to single-process hadd to avoid temp fills.")
                use_parallel = False

        cmd = ["hadd", "-f"]
        if use_parallel:
            cmd += ["-j", str(jobs), "-d", str(chosen_tmp)]
        cmd += [str(output_file), *root_files]

        if not run_command(cmd, True):
            print(
                f"    Error: HADD failed for {sample_key}. Skipping further processing for this entry.",
                file=sys.stderr,
            )
            return False

        source_files = [str(output_file)]

    # Book-keeping for POT/EXT triggers
    if sample_type == "mc" or is_detvar:
        if source_files:
            pot = get_total_pot_from_files_parallel(source_files, jobs)
        else:
            pot = pot_sum_via_iterate(input_dir)
        entry["pot"] = pot if pot != 0.0 else run_pot
    elif sample_type == "ext":
        entry["pot"] = 0.0
        files_for_pairs = source_files if source_files else list(list_root_files(input_dir))
        pairs = _collect_pairs_from_files(list(files_for_pairs))
        total_ext, missing_pairs, by_run = _sum_ext_triggers_from_pairs(run_db, pairs)
        entry["triggers"] = int(total_ext)
        print(f"    EXT triggers (from DB): {total_ext}")
        if not pairs:
            print(
                "    Note: No (run, subrun) pairs found in EXT files; check tree names/paths.",
                file=sys.stderr,
            )
        if missing_pairs:
            print(f"    Note: {len(missing_pairs)} (run,subrun) pairs seen in files not found in DB (showing up to 5): {missing_pairs[:5]}")
    elif sample_type == "data":
        entry["pot"] = run_pot
        entry["triggers"] = ext_triggers

    entry.pop("stage_name", None)
    return True

def load_xml_context(xml_paths: list[Path]) -> tuple[dict[str, str], dict[str, str]]:
    entities: dict[str, str] = {}
    stage_outdirs: dict[str, str] = {}
    for xml in xml_paths:
        text = xml.read_text()
        entities.update(get_xml_entities(xml, text))
        root = ET.fromstring(text)
        proj = root.find("project")
        if proj is None:
            logging.error("Could not find <project> in XML '%s'", xml)
            continue
        for s in proj.findall("stage"):
            outdir_text = s.findtext("outdir") or ""
            for name, val in entities.items():
                outdir_text = outdir_text.replace(f"&{name};", val)
            stage_outdirs[s.get("name")] = outdir_text
    return entities, stage_outdirs

def default_xmls() -> list[Path]:
    return [
        Path("/exp/uboone/app/users/nlane/production/strangeness_mcc9/srcs/ubana/ubana/searchingforstrangeness/xml/numi_fhc_workflow_core.xml"),
        Path("/exp/uboone/app/users/nlane/production/strangeness_mcc9/srcs/ubana/ubana/searchingforstrangeness/xml/numi_fhc_workflow_detvar.xml"),
    ]

def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description="Aggregate ROOT samples from a recipe into a catalogue.")
    ap.add_argument("--recipe", type=Path, required=True, help="Path to recipe JSON (instance).")
    args = ap.parse_args()

    # Fixed defaults (no CLI flags)
    jobs = DEFAULT_JOBS
    run_db = DEFAULT_RUN_DB
    outdir = repo_root / CATALOGUE_SUBDIR

    recipe_path = args.recipe
    with open(recipe_path) as f:
        cfg = json.load(f)
    if cfg.get("role") != "recipe":
        sys.exit(f"Expected role='recipe', found '{cfg.get('role')}'.")
    if cfg.get("recipe_kind", "instance") == "template":
        sys.exit("Refusing to run on a template. Copy it and set recipe_kind='instance'.")

    xml_paths = default_xmls()
    _entities, stage_outdirs = load_xml_context(xml_paths)

    ntuple_dir = Path(cfg["ntuple_base_directory"])
    ntuple_dir.mkdir(parents=True, exist_ok=True)

    beams_in: dict = cfg.get("beamlines", cfg.get("run_configurations", {}))
    beamlines_out: dict = {}

    for beam_key, run_block in beams_in.items():
        beam_active = bool(run_block.get("active", True))
        if not beam_active:
            logging.info("Skipping beam '%s' (active=false).", beam_key)
            continue

        beamline, mode = split_beam_key(beam_key)
        mode = mode.lower()

        for run, run_details in run_block.items():
            if run == "active":
                continue

            logging.info("Processing %s:%s", beam_key, run)
            is_ext = (mode == "ext")
            pot = float(run_details.get("pot", 0.0)) if not is_ext else 0.0
            ext_trig = int(run_details.get("ext_triggers", 0)) if is_ext else 0
            if not is_ext and pot == 0.0:
                logging.warning("No POT provided for %s:%s (on-beam).", beam_key, run)

            samples_in = run_details.get("samples", []) or []
            if not samples_in:
                logging.info("Skipping %s:%s (no samples).", beam_key, run)
                continue

            samples_out = []

            for sample in samples_in:
                s = dict(sample)

                ok = process_sample_entry(
                    s,
                    ntuple_dir,
                    stage_outdirs,
                    pot,
                    ext_trig,
                    run_db,
                    jobs,
                    is_detvar=False,
                )

                if ok and "detector_variations" in s:
                    new_vars = []
                    for dv in s["detector_variations"]:
                        dv2 = dict(dv)
                        process_sample_entry(
                            dv2,
                            ntuple_dir,
                            stage_outdirs,
                            pot,
                            ext_trig,
                            run_db,
                            jobs,
                            is_detvar=True,
                        )
                        new_vars.append(dv2)
                    s["detector_variations"] = new_vars

                samples_out.append(s)

            run_copy = dict(run_details)
            run_copy["samples"] = samples_out
            beamlines_out.setdefault(beam_key, {})[run] = run_copy

    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / "samples.json"

    catalogue = {
        "samples": {
            "ntupledir": cfg["ntuple_base_directory"],
            "beamlines": beamlines_out,
        },
    }

    with open(out_path, "w") as f:
        json.dump(catalogue, f, indent=4)

    logging.info("Wrote catalogue: %s", out_path)

if __name__ == "__main__":
    main()

