#!/usr/bin/env python3
#
# bindingtester_loop.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Run the FoundationDB binding tester against the Swift stack tester in a loop.

Each iteration picks a random seed and test configuration, runs
`bindingtester.py` with the Swift tester (optionally compared against a
reference binding), and records the outcome. The loop ends when either the
time limit or the failure limit is reached. The process exits non-zero if any
run failed, and prints the seeds so the failures can be reproduced locally.

Only the Python standard library is used by this script (the bindingtester
itself needs the `fdb` Python package).
"""

import argparse
import json
import os
import random
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class TestConfig:
    """One flavor of bindingtester run, mirroring run_tester_loop.sh in FDB."""

    name: str
    test_name: str
    num_ops: int
    compare: bool = False
    concurrency: int = 1


API_CONFIGS = [
    TestConfig("api", "api", 1000, compare=True),
    TestConfig("api-concurrent", "api", 1000, concurrency=5),
]

# The Swift bindings do not have a directory layer yet, so these are skipped by
# default. Re-enable them (--include-directory, or fold them into CONFIGS) once
# https://github.com/FoundationDB/fdb-swift-bindings/issues/18 is resolved.
DIRECTORY_CONFIGS = [
    TestConfig("directory", "directory", 1000, compare=True),
    TestConfig("directory-hca", "directory_hca", 100, concurrency=5),
]

CONFIGS = API_CONFIGS + DIRECTORY_CONFIGS


@dataclass
class RunResult:
    config: str
    seed: Optional[int]
    command: List[str]
    passed: bool
    duration: float
    reason: str = ""
    log_file: Optional[str] = None


@dataclass
class Stats:
    results: List[RunResult] = field(default_factory=list)

    @property
    def failures(self) -> List[RunResult]:
        return [r for r in self.results if not r.passed]


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--bindingtester-dir", required=True, type=Path,
                   help="Path to bindings/bindingtester in a FoundationDB checkout")
    p.add_argument("--cluster-file", required=True, help="Cluster file to run against")
    p.add_argument("--tester", default="swift", help="Tester under test (default: swift)")
    p.add_argument("--reference", default="java",
                   help="Reference tester used for --compare runs (default: java)")
    p.add_argument("--time-limit-minutes", type=float, default=60,
                   help="Stop starting new runs after this many minutes (default: 60)")
    p.add_argument("--max-failures", type=int, default=5,
                   help="Stop after this many failed runs (default: 5)")
    p.add_argument("--run-timeout-minutes", type=float, default=15,
                   help="Kill any single bindingtester invocation after this long (default: 15)")
    p.add_argument("--output-dir", type=Path, default=Path("bindingtester-results"),
                   help="Where failure logs and results.json are written")
    p.add_argument("--seed", type=int, default=None,
                   help="Use this seed for the first run instead of a random one (to reproduce a failure)")
    p.add_argument("--meta-seed", type=int, default=None,
                   help="Draw each run's seed from a PRNG seeded with this meta-seed instead of "
                        "from system entropy, so the sequence of seeds is the same every time. "
                        "Incompatible with --seed")
    p.add_argument("--num-seeds", type=int, default=None,
                   help="With --meta-seed: run exactly this many seeds (stopping early only at the "
                        "failure limit) and ignore the time limit, so the set of tests run does not "
                        "depend on how fast they are")
    p.add_argument("--only", choices=[c.name for c in CONFIGS], default=None,
                   help="Restrict the loop to one test configuration")
    p.add_argument("--include-directory", action="store_true",
                   help="Also run the directory layer tests (skipped by default; see "
                        "https://github.com/FoundationDB/fdb-swift-bindings/issues/18)")
    p.add_argument("--skip-scripted", action="store_true",
                   help="Skip the initial scripted test")
    p.add_argument("--python", default=sys.executable, help="Python interpreter used to run bindingtester.py")
    args = p.parse_args(argv)
    # Guard against settings that would silently skip the random loop and report success.
    if args.time_limit_minutes <= 0:
        p.error("--time-limit-minutes must be positive")
    if args.max_failures < 1:
        p.error("--max-failures must be at least 1")
    if args.meta_seed is not None and args.seed is not None:
        p.error("--meta-seed and --seed are mutually exclusive")
    if args.num_seeds is not None and args.meta_seed is None:
        p.error("--num-seeds requires --meta-seed")
    if args.num_seeds is not None and args.num_seeds < 1:
        p.error("--num-seeds must be at least 1")
    if args.seed is not None and args.only is None:
        # The seed is only meaningful for the configuration that produced it.
        p.error("--seed requires --only so the failing configuration is reproduced")
    return args


def run_command(cmd: List[str], timeout_s: float, log_path: Path) -> Tuple[int, str]:
    """Run cmd, teeing combined output to log_path. Returns (exit code, reason)."""
    with open(log_path, "w") as log:
        log.write("$ " + shlex.join(cmd) + "\n\n")
        log.flush()
        # bindingtester.py spawns the tester binaries (Swift and Java) as child
        # processes, and on timeout or interruption we need to kill all of them.
        # `proc.kill()` would only signal the bindingtester itself, leaving its
        # children running as orphans. `os.killpg` kills a whole process group,
        # but by default the child shares *our* group, so using it would kill
        # this script too. start_new_session=True puts the bindingtester (and
        # the children it spawns, which inherit its group) in a fresh group
        # that we can safely killpg without affecting ourselves.
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return proc.wait(timeout=timeout_s), ""
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
            return -1, f"timed out after {timeout_s:.0f}s"
        except BaseException:
            # Interrupted (e.g. SIGTERM from a job cancellation): don't leave testers running.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            raise


def tail(path: Path, lines: int = 40) -> str:
    try:
        return "".join(path.read_text(errors="replace").splitlines(keepends=True)[-lines:])
    except OSError:
        return ""


def main(argv) -> int:
    args = parse_args(argv)
    # Turn SIGTERM into SystemExit so cleanup and reporting below still happen.
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(128 + signum))
    bt = args.bindingtester_dir.resolve() / "bindingtester.py"
    if not bt.is_file():
        print(f"error: {bt} does not exist", file=sys.stderr)
        return 2

    out = args.output_dir.resolve()
    (out / "failures").mkdir(parents=True, exist_ok=True)
    scratch = out / "scratch.log"

    base = [args.python, "-u", str(bt), args.tester,
            "--cluster-file", args.cluster_file, "--logging-level", "INFO"]
    stats = Stats()
    run_timeout = args.run_timeout_minutes * 60
    deadline = time.monotonic() + args.time_limit_minutes * 60
    enabled = CONFIGS if args.include_directory else API_CONFIGS
    configs = [c for c in enabled if args.only in (None, c.name)]
    if not configs:
        print(f"error: --only {args.only} is not enabled (try --include-directory)", file=sys.stderr)
        return 2
    next_seed = args.seed

    def command_for(config: TestConfig, seed: int) -> List[str]:
        cmd = base + ["--test-name", config.test_name, "--num-ops", str(config.num_ops),
                      "--seed", str(seed)]
        if config.concurrency > 1:
            cmd += ["--concurrency", str(config.concurrency)]
        if config.compare:
            cmd += ["--compare", args.reference]
        return cmd

    def record(config: str, seed: Optional[int], cmd: List[str]) -> RunResult:
        start = time.monotonic()
        code, reason = run_command(cmd, run_timeout, scratch)
        duration = time.monotonic() - start
        passed = code == 0 and not reason
        if code != 0 and not reason:
            reason = f"exit code {code}"
        result = RunResult(config, seed, cmd, passed, duration, reason)
        if not passed:
            name = f"{config}-seed-{seed}" if seed is not None else config
            dest = out / "failures" / f"{name}.log"
            scratch.replace(dest)
            result.log_file = str(dest.relative_to(out))
            repro = shlex.join(["python3", "bindingtester.py"] + cmd[3:])
            print(f"::error title=bindingtester failure ({config})::"
                  f"{reason}; seed={seed}; reproduce with: {repro}")
            print(f"--- last lines of {dest.name} ---\n{tail(dest)}--- end ---", flush=True)
        stats.results.append(result)
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {config} seed={seed} ({duration:.1f}s)", flush=True)
        return result

    if not args.skip_scripted:
        print("== scripted test ==", flush=True)
        record("scripted", None, base + ["--test-name", "scripted"])

    print("== enabled configurations: " + ", ".join(c.name for c in configs)
          + ("" if args.include_directory else " (directory tests skipped, see issue #18)"), flush=True)
    if args.meta_seed is not None:
        # A dedicated, seeded PRNG (not SystemRandom) so the same meta-seed always yields the
        # same sequence of seeds. Only getrandbits is used, as its output is stable across
        # Python versions. Each run's seed is printed, and appears in its repro command.
        prng = random.Random(args.meta_seed)
        next_random_seed = lambda: prng.getrandbits(32)
        seed_source = f"seeds from meta-seed {args.meta_seed}"
    else:
        sysrandom = random.SystemRandom()
        next_random_seed = lambda: sysrandom.getrandbits(32)
        seed_source = "random seeds"
    if args.num_seeds is not None:
        limit = f"{args.num_seeds} runs"
    else:
        limit = f"{args.time_limit_minutes} min"
    print(f"== loop ({seed_source}): up to {limit} / {args.max_failures} failures ==", flush=True)
    iteration = 0
    stopped = "run count" if args.num_seeds is not None else "time limit"

    def keep_going() -> bool:
        if len(stats.failures) >= args.max_failures:
            return False
        if args.num_seeds is not None:
            return iteration < args.num_seeds
        return time.monotonic() < deadline

    try:
        while keep_going():
            config = configs[iteration % len(configs)]
            iteration += 1
            seed = next_seed if next_seed is not None else next_random_seed()
            next_seed = None
            record(config.name, seed, command_for(config, seed))
        if len(stats.failures) >= args.max_failures:
            stopped = "failure limit"
    except BaseException:
        stopped = "interrupted"
        raise
    finally:
        # Always leave a report behind, even if cancelled part-way through.
        scratch.unlink(missing_ok=True)
        write_reports(out, stats, stopped, args)
        failures = stats.failures
        print(f"\n{len(stats.results)} runs, {len(failures)} failed (stopped at {stopped}).")
        for f in failures:
            print(f"  FAILED {f.config} seed={f.seed}: {f.reason}")
    return 1 if failures else 0


def write_reports(out: Path, stats: Stats, stopped: str, args) -> None:
    results = stats.results
    (out / "results.json").write_text(json.dumps({
        "tester": args.tester,
        "reference": args.reference,
        "stopped_by": stopped,
        "runs": len(results),
        "failures": [r.__dict__ for r in stats.failures],
    }, indent=2))

    lines = [
        f"## bindingtester: `{args.tester}` vs `{args.reference}`",
        "",
        f"- Runs: **{len(results)}** (stopped by {stopped})",
        f"- Failures: **{len(stats.failures)}**",
        "",
    ]
    if stats.failures:
        lines += ["| Test | Seed | Reason | Reproduce |", "|---|---|---|---|"]
        for f in stats.failures:
            repro = shlex.join(["python3", "bindingtester.py"] + f.command[3:])
            lines.append(f"| {f.config} | {f.seed} | {f.reason} | `{repro}` |")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as fh:
            fh.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
