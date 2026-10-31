"""Inventory and verify local/pinned OpenAPI clients. Run with --help for usage."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import shutil
import signal
import threading
import yaml
from openapi_yaml import load as load_yaml, dumps as dump_yaml_json

ROOT = Path(__file__).resolve().parents[1]
DRIVER = ROOT / "test/Soenneker.OpenApi.Fixer.Verification/bin/Debug/net11.0/Soenneker.OpenApi.Fixer.Verification.dll"
PACKAGES = ["Http.HttpClientLibrary", "Serialization.Form", "Serialization.Json", "Serialization.Multipart", "Serialization.Text"]
SOURCE_LOCK = threading.Lock()
PREPARED = {}


def source_hash(path):
    # Git's checkout line-ending conversion must not make Windows and Linux inventories disagree.
    text = path.read_text(encoding="utf-8-sig").replace("\r\n", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def git(directory, *args):
    return subprocess.check_output(["git", "-C", str(directory), *args], text=True).strip()


def inventory(workspace, output):
    files = subprocess.check_output(["rg", "--files", str(workspace), "-g", "kiota-lock.json"], text=True).splitlines()
    projects = subprocess.check_output(["rg", "--files", str(workspace), "-g", "*.csproj"], text=True).splitlines()
    files.extend(p for p in projects if Path(p).name.lower().endswith(".openapiclient.csproj") and ".runners." not in p.lower() and ".runner." not in p.lower())
    repositories = {}
    for file in files:
        lock = Path(file)
        repository = next((p for p in lock.parents if (p / ".git").exists()), None)
        if repository is None or repository in repositories:
            continue
        candidates = []
        source_files = []
        for folder, dirs, names in os.walk(repository):
            dirs[:] = [d for d in dirs if d not in (".git", ".github", "src", "test", "bin", "obj", "node_modules")]
            source_files.extend(Path(folder) / name for name in names if name.lower().endswith((".json", ".yaml", ".yml")))
        for path in source_files:
            try:
                raw = path.read_text(encoding="utf-8-sig")
                document = json.loads(raw) if path.suffix.lower() == ".json" else load_yaml(raw)
                if isinstance(document, dict) and ("openapi" in document or "swagger" in document):
                    candidates.append(path)
            except (ValueError, OSError, yaml.YAMLError):
                pass
        originals = [p for p in candidates if "fixed" not in p.name.lower()]
        candidates = sorted(originals or candidates, key=lambda p: (p.name != "merged-openapi.json", p.name not in ("openapi.json", "spec3.json", "swagger.json"), p.as_posix()))
        source = candidates[0] if candidates else None
        repositories[repository] = {
            "name": repository.name,
            "repository": git(repository, "remote", "get-url", "origin"),
            "revision": git(repository, "rev-parse", "HEAD"),
            "directory": repository.relative_to(workspace).as_posix(),
            "source": source.relative_to(repository).as_posix() if source else None,
            "sourceKind": "original" if originals else "normalized-only" if source else "missing",
            "sha256": source_hash(source) if source else None,
            "additionalSources": [{"source": p.relative_to(repository).as_posix(), "sha256": source_hash(p)}
                                  for p in candidates[1:] if "fixed" not in p.name.lower()],
        }
    entries = sorted(repositories.values(), key=lambda e: e["name"])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"repositories": len(entries), "original": sum(e["sourceKind"] == "original" for e in entries),
                      "normalizedOnly": sum(e["sourceKind"] == "normalized-only" for e in entries),
                      "missing": [e["name"] for e in entries if not e["source"]]}, indent=2))


def run(command, log, timeout=900, processor_count=None):
    with log.open("w", encoding="utf-8") as stream:
        environment = os.environ.copy()
        if processor_count is not None:
            environment["DOTNET_PROCESSOR_COUNT"] = str(processor_count)
        process = subprocess.Popen(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                                   env=environment,
                                   creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
                                   start_new_session=os.name != "nt")
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], stdout=stream, stderr=subprocess.STDOUT)
            else:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise RuntimeError(f"Timed out after {timeout}s: {log.name}")
    if code:
        raise RuntimeError(f"Exit {code}: {log.name}")


def verify(entry, workspace, output, driver):
    started = time.monotonic()
    directory = output / entry["name"]
    directory.mkdir(parents=True, exist_ok=True)
    result = {"name": entry["name"], "sourceKind": entry["sourceKind"], "revision": entry["revision"], "stage": "source", "passed": False}
    try:
        if not entry["source"]:
            raise RuntimeError("No JSON specification available")
        if workspace:
            repository = workspace / entry["directory"]
        else:
            repository = output / "sources" / entry["name"].split("--")[0]
            with SOURCE_LOCK:
                if not (repository / ".git").exists():
                    run(["git", "clone", "--no-checkout", "--filter=blob:none", entry["repository"], str(repository)], directory / "checkout.log")
                    run(["git", "-C", str(repository), "checkout", "--detach", entry["revision"]], directory / "revision.log")
        source = repository / entry["source"]
        if source_hash(source) != entry["sha256"]:
            raise RuntimeError("Source hash differs from pinned inventory; refresh inventory deliberately")
        fixed = directory / "fixed.json"
        if source.suffix.lower() in (".yaml", ".yml"):
            # Keep relative references intact, including YAML dependencies whose filenames must remain unchanged.
            with SOURCE_LOCK:
                if repository not in PREPARED:
                    converted = output / "converted" / entry["name"].split("--")[0]
                    for folder, dirs, names in os.walk(repository):
                        dirs[:] = [d for d in dirs if d not in (".git", ".github", "src", "test", "bin", "obj", "node_modules")]
                        for name in names:
                            original = Path(folder) / name
                            if original.suffix.lower() not in (".json", ".yaml", ".yml"): continue
                            destination = converted / original.relative_to(repository)
                            destination.parent.mkdir(parents=True, exist_ok=True)
                            raw = original.read_text(encoding="utf-8-sig")
                            try:
                                converted_text = dump_yaml_json(load_yaml(raw)) if original.suffix.lower() != ".json" else raw
                            except (ValueError, yaml.YAMLError):
                                converted_text = raw
                            destination.write_text(converted_text, encoding="utf-8")
                    PREPARED[repository] = converted
                source = PREPARED[repository] / entry["source"]
        result["stage"] = "fix"
        run(["dotnet", str(driver), "fix", str(source), str(fixed)], directory / "fix.log")
        provider_contracts = json.loads((ROOT / "test/Soenneker.OpenApi.Fixer.Verification/Fixtures/provider-contracts.json").read_text(encoding="utf-8"))
        if entry["name"] in provider_contracts:
            document = json.loads(fixed.read_text(encoding="utf-8-sig"))
            for model, payloads in provider_contracts[entry["name"]].items():
                document["components"]["schemas"][model]["x-verification-provider-payloads"] = payloads
            fixed.write_text(json.dumps(document, indent=2), encoding="utf-8")
        generated = directory / "generated"
        # A unique run directory prevents stale generated files from hiding removed models.
        if generated.exists():
            raise RuntimeError("Output already exists; use a new --output directory")
        result["stage"] = "generate"
        # Kiota 1.37.0's parallel MissingDiscriminator validator can race while appending warnings.
        # Isolate it to one processor; independent specifications still run concurrently.
        run(["dotnet", "tool", "run", "kiota", "generate", "-d", str(fixed), "-o", str(generated), "-l", "CSharp",
             "-c", "ApiClient", "-n", "Verification.Client", "--ebc", "--ll", "Warning"], directory / "generate.log", processor_count=1)
        if not (generated / "ApiClient.cs").exists():
            raise RuntimeError("Kiota did not produce a client")
        result["stage"] = "repair"
        run(["dotnet", str(driver), "repair", str(generated)], directory / "repair.log")
        result["stage"] = "audit"
        run(["dotnet", str(driver), "audit", str(fixed), str(generated)], directory / "audit.log")
        result["stage"] = "compile"
        project = directory / "Client.csproj"
        references = "".join(f'<PackageReference Include="Microsoft.Kiota.{p}" Version="2.1.2" />' for p in PACKAGES)
        shutil.copyfile(ROOT / "tools/RuntimeProbe.cs", directory / "RuntimeProbe.cs")
        (directory / "Program.cs").write_text('await RuntimeProbe.Run(args[0]);', encoding="utf-8")
        project.write_text('<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><TargetFramework>net11.0</TargetFramework><OutputType>Exe</OutputType><ImplicitUsings>enable</ImplicitUsings>'
                           '<Nullable>enable</Nullable><EnableDefaultCompileItems>false</EnableDefaultCompileItems>'
                           '</PropertyGroup><ItemGroup><Compile Include="generated/**/*.cs" /><Compile Include="Program.cs" /><Compile Include="RuntimeProbe.cs" />' + references + '</ItemGroup></Project>', encoding="utf-8")
        run(["dotnet", "build", str(project), "--verbosity", "quiet", "-p:UseSharedCompilation=false"], directory / "build.log")
        result["stage"] = "runtime"
        run(["dotnet", str(directory / "bin/Debug/net11.0/Client.dll"), str(fixed)], directory / "runtime.log")
        result.update(passed=True, stage="complete", generatedFiles=len(list(generated.rglob("*.cs"))))
    except Exception as error:
        result["error"] = str(error)
    result["seconds"] = round(time.monotonic() - started, 1)
    (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["inventory", "verify", "validate-inventory"])
    parser.add_argument("--workspace", type=Path, help="Local repository tree; omitted verification fetches pinned repositories")
    parser.add_argument("--manifest", type=Path, default=ROOT / "test/client-matrix.json")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/client-verification")
    parser.add_argument("--providers", nargs="*", help="Exact repository names or provider segments, e.g. cloudflare stripe")
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--driver", type=Path, default=DRIVER)
    parser.add_argument("--contracts-only", action="store_true", help="Generate, compile and round-trip the checked-in behavioral contracts")
    args = parser.parse_args()
    if args.command == "inventory":
        if not args.workspace:
            parser.error("inventory requires --workspace")
        inventory(args.workspace.resolve(), args.manifest.resolve())
        return
    if args.command == "validate-inventory":
        if not args.workspace:
            parser.error("validate-inventory requires --workspace")
        mismatches = []
        for entry in json.loads(args.manifest.read_text(encoding="utf-8")):
            for item in [entry, *entry.get("additionalSources", [])]:
                if not item["source"]:
                    mismatches.append(entry["name"] + ": missing source")
                    continue
                try:
                    data = subprocess.check_output(["git", "-C", str(args.workspace / entry["directory"]), "show",
                                                    entry["revision"] + ":" + item["source"]], stderr=subprocess.DEVNULL)
                    actual = hashlib.sha256(data.decode("utf-8-sig").replace("\r\n", "\n").encode("utf-8")).hexdigest()
                    if actual != item["sha256"]:
                        mismatches.append(entry["name"] + ": " + item["source"])
                except subprocess.CalledProcessError:
                    mismatches.append(entry["name"] + ": untracked " + item["source"])
        print(json.dumps({"mismatches": mismatches}, indent=2))
        raise SystemExit(bool(mismatches))
    if args.contracts_only:
        source = ROOT / "test/Soenneker.OpenApi.Fixer.Verification/Fixtures/contracts.json"
        args.workspace = ROOT
        entries = [{"name": "contracts", "directory": ".", "source": source.relative_to(ROOT).as_posix(),
                    "sourceKind": "contract", "revision": git(ROOT, "rev-parse", "HEAD"),
                    "sha256": source_hash(source)}]
    else:
        entries = json.loads(args.manifest.read_text(encoding="utf-8"))
        expanded = []
        for entry in entries:
            expanded.append(entry)
            for additional in entry.get("additionalSources", []):
                expanded.append({**entry, **additional, "name": entry["name"] + "--" + additional["source"].replace("/", "--"),
                                 "additionalSources": []})
        entries = expanded
    if args.providers:
        requested = set(args.providers)
        entries = [e for e in entries if e["name"].split("--")[0] in requested or requested.intersection(e["name"].split(".")[:2])]
        if not entries:
            parser.error("No providers matched")
    priority = ["soenneker." + provider + ".openapiclient" for provider in ("cloudflare", "stripe", "telnyx", "github", "workos")]
    entries.sort(key=lambda entry: priority.index(entry["name"]) if entry["name"] in priority else len(priority))
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    driver_directory = args.output / "driver"
    shutil.copytree(args.driver.resolve().parent, driver_directory, dirs_exist_ok=True)
    driver = driver_directory / args.driver.name
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as executor:
        futures = [executor.submit(verify, e, args.workspace.resolve() if args.workspace else None, args.output, driver) for e in entries]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(0 if all(r["passed"] for r in results) else 1)


if __name__ == "__main__":
    main()
