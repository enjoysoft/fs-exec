#!/usr/bin/env python3
"""Example manifest gate; OS/DB permissions must enforce actual read-only access."""
import json
import pathlib
import sys

request = json.load(sys.stdin)
argv = request.get("argv", [])
executable = pathlib.Path(argv[0]).name.lower() if argv else ""
if request.get("mode") != "argv" or executable not in {"curl", "psql"}:
    print("only argv-mode curl and psql are approved", file=sys.stderr)
    raise SystemExit(2)
if executable == "psql" and any(value in {"-f", "--file"} for value in argv[1:]):
    print("psql script files are not approved", file=sys.stderr)
    raise SystemExit(2)
