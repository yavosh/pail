#!/usr/bin/env bash
# Rerun every check listed in checks.txt and *.checks next to this script, and
# compare each result with its expected outcome.
# Line format: <spec> <config> <pass|fail> [note...]
#   TLC line:  <spec> is a module, <config> is its .cfg file.
#   Lean line: <spec> is the word "lean", <config> is a Lake project directory.
# A TLC "fail" expectation passes only on a real property violation or a
# deadlock, never on a parse or setup error. A Lean check passes only when the
# project builds and Audit.lean shows no sorry and no axioms beyond propext,
# Quot.sound, and Classical.choice. Exits non-zero if any result differs.
set -euo pipefail
cd "$(dirname "$0")"

files=()
for f in checks.txt *.checks; do [[ -f "$f" ]] && files+=("$f"); done
[[ ${#files[@]} -gt 0 ]] || { echo "no checks.txt or *.checks next to check.sh" >&2; exit 2; }

meta="$(mktemp -d)"
trap 'rm -rf "$meta"' EXIT
status=0
checks=0

run_tlc() {
  if [[ -z "${JAVA:-}" ]]; then
    if command -v brew >/dev/null && [[ -x "$(brew --prefix openjdk 2>/dev/null)/bin/java" ]]; then
      JAVA="$(brew --prefix openjdk)/bin/java"
    else
      JAVA=java
    fi
  fi
  local jar="${TLA2TOOLS:-$HOME/.local/share/tla/tla2tools.jar}"
  [[ -f "$jar" ]] || { got=error; detail="tla2tools.jar not found at $jar (set TLA2TOOLS)"; return; }
  # No -deadlock flag: it overrides CHECK_DEADLOCK in the .cfg.
  out="$("$JAVA" -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto \
    -metadir "$meta/$cfg" -config "$cfg" "$spec" </dev/null 2>&1 || true)"
  detail="$(grep -oE '(Invariant|Action property|Temporal properties) [A-Za-z0-9_]* ?(is|were) violated|Deadlock reached' <<<"$out" | head -1 || true)"
  if grep -q 'No error has been found' <<<"$out"; then got=pass
  elif [[ -n "$detail" ]]; then got=fail
  else got=error; detail="$(grep -m3 -iE 'error|exception' <<<"$out" | tr '\n' ' ' || true)"
  fi
}

run_lean() {
  [[ -f "$cfg/Audit.lean" ]] || { got=error; detail="$cfg/Audit.lean not found"; return; }
  if ! out="$(cd "$cfg" && lake build </dev/null 2>&1 && lake env lean Audit.lean </dev/null 2>&1)"; then
    got=error; detail="$(grep -m3 -iE 'error' <<<"$out" | tr '\n' ' ' || true)"; return
  fi
  local axioms bad
  axioms="$(grep -oE 'depends on axioms: \[[^]]*\]|does not depend on any axioms' <<<"$out" || true)"
  bad="$(sed -n 's/.*\[\(.*\)\]/\1/p' <<<"$axioms" | tr ',' '\n' | tr -d ' ' \
    | grep -v '^$' | grep -vxE 'propext|Quot\.sound|Classical\.choice' | sort -u | tr '\n' ' ' || true)"
  if [[ -z "$axioms" ]]; then got=error; detail="Audit.lean printed no axioms"
  elif grep -q "declaration uses 'sorry'" <<<"$out"; then got=error; detail="uses sorry"
  elif [[ -n "$bad" ]]; then got=error; detail="extra axioms: $bad"
  else got=pass; detail=""
  fi
}

for f in "${files[@]}"; do
  while read -r spec cfg expect note || [[ -n "${spec:-}" ]]; do
    [[ -z "${spec:-}" || "$spec" == \#* ]] && continue
    if [[ -z "$cfg" || ( "$expect" != pass && "$expect" != fail ) ]]; then
      printf 'invalid check in %s: expected <spec> <config> <pass|fail> [note...]\n' "$f" >&2
      status=1
      continue
    fi
    checks=$((checks + 1))
    got=error detail=""
    if [[ "$spec" == lean ]]; then run_lean; else run_tlc; fi
    mark=ok
    [[ "$got" == "$expect" ]] || { mark=BAD; status=1; }
    if [[ "$got" == error ]]; then
      printf '%-3s  %-36s expect=%-4s got=%-5s %s\n' "$mark" "$cfg" "$expect" "$got" "${note:-}"
      printf '     %s\n' "$detail"
    else
      printf '%-3s  %-36s expect=%-4s got=%-5s %s\n' "$mark" "$cfg" "$expect" "$got" "${detail:-${note:-}}"
    fi
  done < "$f"
done
[[ "$checks" -gt 0 ]] || { echo "no executable checks found" >&2; exit 2; }
exit "$status"
