"""Write the nft JSON fixtures tests/ip-forward.sh feeds to the fake nft.

    python3 ip-forward-fixtures.py <output-dir>

The shapes come from test_forward_guard.py, which mirrors what the real nft
prints for the hardening template.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys

here = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("bbraft_forward_guard_tests", here / "test_forward_guard.py")
tests = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tests)


def without_forward_rules(doc):
    doc["nftables"] = [i for i in doc["nftables"] if i.get("rule", {}).get("chain") != "forward"]
    return doc


def main() -> None:
    out = pathlib.Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    extra_rule = tests.ruleset()
    extra_rule["nftables"].append(tests.accept_rule("iifname", "other0"))
    generic = tests.ruleset()
    generic["nftables"].append(
        {"rule": {"family": "inet", "table": "bbraft_filter", "chain": "forward", "handle": 99,
                  "expr": [{"accept": None}]}}
    )
    ipv6 = tests.ruleset()
    for item in ipv6["nftables"]:
        rule = item.get("rule")
        if rule and rule["chain"] == "forward":
            rule["expr"][0]["match"]["right"] = "ipv6"
    no_table = {"nftables": [{"metainfo": {"version": "1.1.7"}}]}
    fixtures = {
        "good": tests.ruleset(),
        "policy-accept": tests.ruleset(forward_policy="accept"),
        "priority": tests.ruleset(forward_prio=-1),
        "extra-rule": extra_rule,
        "generic-accept": generic,
        "ipv6": ipv6,
        "no-rules": without_forward_rules(tests.ruleset()),
        "no-table": no_table,
    }
    for name, doc in fixtures.items():
        (out / f"{name}.json").write_text(json.dumps(doc))
    (out / "garbage.json").write_text("not json")
    (out / "duplicate-key.json").write_text(
        json.dumps(tests.ruleset()).replace('"policy": "drop"', '"policy": "accept", "policy": "drop"', 1)
    )


if __name__ == "__main__":
    main()
