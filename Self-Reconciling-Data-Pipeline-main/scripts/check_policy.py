#!/usr/bin/env python3
"""CI check for policy/policy.yml and agents/registry.yml.

Fails when:
  * a rule uses an action that is not in the catalog
  * a never-auto action appears in the catalog or in any rule
  * an `auto` rule uses an action whose catalog mode is `approve`
  * the last ordinary rule is not the fallback (when: {any: true})
  * rule ids repeat, severities or routes are unknown
  * agents are enabled while residency is not confirmed
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ROUTES = {"auto", "L2", "L3"}
SEVERITIES = {"P1", "P2", "P3"}


def check(policy: dict, registry: dict) -> list[str]:
    errors: list[str] = []
    if not re.fullmatch(r"\d+\.\d+\.\d+(-[\w.]+)?", str(policy.get("version", ""))):
        errors.append("version must be semver, e.g. 1.7.0 or 1.7.0-poc.1")
    if not isinstance(policy.get("kill_switch"), bool):
        errors.append("kill_switch must be true or false")

    catalog = policy.get("actions") or {}
    never = set(policy.get("never_auto") or [])
    for name in never & set(catalog):
        errors.append(f"never-auto action '{name}' must not be in the action catalog")
    for name, a in catalog.items():
        if a.get("mode") not in ("auto", "approve"):
            errors.append(f"action '{name}': mode must be auto or approve")

    rules = policy.get("rules") or {}
    seen: set[str] = set()
    for stage in ("safety_overrides", "ordinary"):
        for r in rules.get(stage) or []:
            rid = r.get("id", "?")
            if rid in seen:
                errors.append(f"duplicate rule id '{rid}'")
            seen.add(rid)
            act = r.get("action")
            if act in never:
                errors.append(f"rule '{rid}' uses never-auto action '{act}'")
            elif act not in catalog:
                errors.append(f"rule '{rid}' uses unknown action '{act}'")
            elif r.get("route") == "auto" and catalog[act].get("mode") == "approve":
                errors.append(f"rule '{rid}' auto-runs '{act}', which needs approval")
            if r.get("route") not in ROUTES:
                errors.append(f"rule '{rid}': route must be one of {sorted(ROUTES)}")
            if r.get("severity") not in SEVERITIES:
                errors.append(f"rule '{rid}': severity must be one of {sorted(SEVERITIES)}")
            if not r.get("when"):
                errors.append(f"rule '{rid}' has no 'when'")

    ordinary = rules.get("ordinary") or []
    if not ordinary or ordinary[-1].get("when") != {"any": True}:
        errors.append("the last ordinary rule must be the fallback: when: {any: true}")
    for r in ordinary[:-1]:
        if r.get("when") == {"any": True}:
            errors.append(f"fallback rule '{r.get('id')}' must be last")

    agents = policy.get("agents") or {}
    if agents.get("enabled") and not agents.get("residency_confirmed"):
        errors.append("agents.enabled is true but agents.residency_confirmed is false")
    if not 0 < float(agents.get("min_confidence", 0)) <= 1:
        errors.append("agents.min_confidence must be in (0, 1]")

    for a in registry.get("agents") or []:
        for k in ("id", "model", "prompt_version", "token_budget", "timeout_seconds"):
            if k not in a:
                errors.append(f"agent registry entry {a.get('id', '?')}: missing '{k}'")
        for act in a.get("may_propose") or []:
            if act not in catalog:
                errors.append(f"agent '{a.get('id')}' may propose unknown action '{act}'")
    return errors


def main() -> int:
    policy = yaml.safe_load((ROOT / "policy" / "policy.yml").read_text())
    registry = yaml.safe_load((ROOT / "agents" / "registry.yml").read_text())
    errors = check(policy, registry)
    for e in errors:
        print(f"policy: {e}")
    if not errors:
        n = sum(len(v or []) for v in policy["rules"].values())
        print(f"policy {policy['version']}: OK ({len(policy['actions'])} actions, {n} rules, "
              f"kill switch {'ON' if policy['kill_switch'] else 'off'}, "
              f"agents {'enabled' if policy['agents']['enabled'] else 'off'})")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
