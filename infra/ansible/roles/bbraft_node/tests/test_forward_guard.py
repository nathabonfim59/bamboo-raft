"""Unit tests for bbraft_node_forward_guard, the live-nft guard that must hold
before the role persists and applies net.ipv4.ip_forward=1.

Run directly with python3; no Ansible, nft or root is needed:

    python3 roles/bbraft_node/tests/test_forward_guard.py

The JSON shapes below are the ones ``nft -j list ruleset`` prints for the
hardening role's nftables template (checked against a real nft in a private
network namespace by tests/ip-forward.sh through tests/ip-forward-netns.sh).
"""

from __future__ import annotations

import copy
import importlib.util
import json
import pathlib
import unittest

MODULE_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "filter_plugins"
    / "bbraft_node_filters.py"
)
SPEC = importlib.util.spec_from_file_location("bbraft_node_filters", MODULE_PATH)
FILTERS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FILTERS)

BRIDGE = "bbraft0"
SSH = "example0"


def chain(name, hook=None, policy=None, prio=0, family="inet", table="bbraft_filter", **extra):
    body = {"family": family, "table": table, "name": name, "handle": 1}
    if hook is not None:
        body.update({"type": "filter", "hook": hook, "prio": prio, "policy": policy})
    body.update(extra)
    return {"chain": body}


def meta_match(key, value, op="=="):
    return {"match": {"op": op, "left": {"meta": {"key": key}}, "right": value}}


def accept_rule(direction, iface, chain_name="forward", proto="ipv4"):
    return {
        "rule": {
            "family": "inet",
            "table": "bbraft_filter",
            "chain": chain_name,
            "handle": 14,
            "expr": [
                meta_match("nfproto", proto),
                meta_match(direction, iface),
                {"accept": None},
            ],
        }
    }


def input_rules():
    """Representative input-chain rules of the hardening template."""
    drop = {"match": {"op": "in", "left": {"ct": {"key": "state"}}, "right": "invalid"}}
    return [
        {"rule": {"family": "inet", "table": "bbraft_filter", "chain": "input", "handle": 4,
                  "expr": [drop, {"drop": None}]}},
        {"rule": {"family": "inet", "table": "bbraft_filter", "chain": "input", "handle": 6,
                  "expr": [meta_match("iifname", "tailscale0"), {"accept": None}]}},
        {"rule": {"family": "inet", "table": "bbraft_filter", "chain": "input", "handle": 10,
                  "expr": [{"limit": {"rate": 5, "burst": 5, "per": "second"}}, {"accept": None}]}},
    ]


def ruleset(bridges=(BRIDGE,), forward_policy="drop", extra=(), forward_prio=0):
    items = [
        {"metainfo": {"version": "1.1.7", "release_name": "x", "json_schema_version": 1}},
        {"table": {"family": "inet", "name": "bbraft_filter", "handle": 10}},
        chain("input", "input", "drop"),
        chain("forward", "forward", forward_policy, prio=forward_prio),
        *input_rules(),
    ]
    for iface in bridges:
        items.append(accept_rule("iifname", iface))
        items.append(accept_rule("oifname", iface))
    items.extend(extra)
    return {"nftables": items}


def guard(doc, bridges=(BRIDGE,), bridge=BRIDGE, ssh=SSH):
    text = doc if isinstance(doc, str) else json.dumps(doc)
    return FILTERS.bbraft_node_forward_guard(text, list(bridges), bridge, ssh)


class ForwardGuardTest(unittest.TestCase):
    def refused(self, doc, want, **kwargs):
        with self.assertRaises(ValueError) as ctx:
            guard(doc, **kwargs)
        self.assertIn(want, str(ctx.exception))

    def forward_rules(self, doc):
        return [i for i in doc["nftables"] if "rule" in i and i["rule"]["chain"] == "forward"]

    def test_hardening_shape_is_accepted(self):
        self.assertTrue(guard(ruleset()))

    def test_two_bridges_and_order_do_not_matter(self):
        doc = ruleset(bridges=("br1", BRIDGE))
        self.assertTrue(guard(doc, bridges=(BRIDGE, "br1")))
        rules = self.forward_rules(doc)
        rules.reverse()
        rest = [i for i in doc["nftables"] if i not in rules]
        self.assertTrue(guard({"nftables": rest + rules}, bridges=(BRIDGE, "br1")))

    def test_other_tables_and_families_are_ignored(self):
        other = [
            {"table": {"family": "ip", "name": "nat", "handle": 1}},
            chain("forward", "forward", "accept", table="tailscale", family="ip"),
            {"rule": {"family": "ip", "table": "tailscale", "chain": "forward", "handle": 3,
                      "expr": [{"accept": None}]}},
        ]
        self.assertTrue(guard(ruleset(extra=other)))

    def test_missing_target_is_refused(self):
        self.refused({"nftables": [{"metainfo": {"version": "1.1.7"}}]}, "no inet bbraft_filter forward chain")
        doc = ruleset()
        doc["nftables"] = [i for i in doc["nftables"] if i.get("chain", {}).get("name") != "forward"]
        doc["nftables"] = [i for i in doc["nftables"] if i.get("rule", {}).get("chain") != "forward"]
        self.refused(doc, "no inet bbraft_filter forward chain")

    def test_duplicate_target_is_refused(self):
        doc = ruleset()
        doc["nftables"].append(chain("forward", "forward", "drop"))
        self.refused(doc, "more than one")
        doc = ruleset()
        doc["nftables"].append(chain("fwd2", "forward", "drop"))
        self.refused(doc, "more than one")
        doc = ruleset()
        doc["nftables"].append({"table": {"family": "inet", "name": "bbraft_filter", "handle": 11}})
        self.refused(doc, "more than one")

    def test_wrong_policy_priority_type_hook(self):
        self.refused(ruleset(forward_policy="accept"), "policy")
        self.refused(ruleset(forward_prio=-1), "priority")
        self.refused(ruleset(forward_prio=1), "priority")
        doc = ruleset()
        for item in doc["nftables"]:
            if item.get("chain", {}).get("name") == "forward":
                item["chain"]["type"] = "route"
        self.refused(doc, "type")
        doc = ruleset()
        for item in doc["nftables"]:
            if item.get("chain", {}).get("name") == "forward":
                del item["chain"]["policy"]
        self.refused(doc, "policy")
        doc = ruleset()
        for item in doc["nftables"]:
            if item.get("chain", {}).get("name") == "forward":
                item["chain"]["dev"] = "eth0"
        self.refused(doc, "unexpected")

    def test_forward_name_without_forward_hook_is_refused(self):
        doc = ruleset()
        for item in doc["nftables"]:
            if item.get("chain", {}).get("name") == "forward":
                item["chain"] = {"family": "inet", "table": "bbraft_filter", "name": "forward", "handle": 2}
        self.refused(doc, "no inet bbraft_filter forward chain")

    def test_extra_or_missing_rules_are_refused(self):
        doc = ruleset()
        doc["nftables"].append(accept_rule("iifname", "other0"))
        self.refused(doc, "other0")

        doc = ruleset()
        doc["nftables"].append(accept_rule("iifname", BRIDGE))
        self.refused(doc, "exactly 2")

        doc = ruleset()
        rules = self.forward_rules(doc)
        doc["nftables"].remove(rules[1])
        self.refused(doc, "exactly 2")

        doc = ruleset()
        for rule in self.forward_rules(doc):
            doc["nftables"].remove(rule)
        self.refused(doc, "exactly 2")

    def test_both_directions_are_required(self):
        doc = ruleset()
        rules = self.forward_rules(doc)
        doc["nftables"].remove(rules[1])
        doc["nftables"].append(accept_rule("iifname", BRIDGE))
        self.refused(doc, "exactly 2")

    def test_rule_shape_negatives(self):
        def mutate(fn):
            doc = ruleset()
            fn(self.forward_rules(doc)[0]["rule"])
            return doc

        cases = {
            "generic accept": lambda r: r.update(expr=[{"accept": None}]),
            "no nfproto": lambda r: r.update(expr=r["expr"][1:]),
            "ipv6": lambda r: r["expr"][0]["match"].update(right="ipv6"),
            "inequality": lambda r: r["expr"][1]["match"].update(op="!="),
            "wrong iface": lambda r: r["expr"][1]["match"].update(right="evil0"),
            "iif not iifname": lambda r: r["expr"][1]["match"]["left"]["meta"].update(key="iif"),
            "counter": lambda r: r["expr"].insert(2, {"counter": {"packets": 0, "bytes": 0}}),
            "jump": lambda r: r.update(expr=r["expr"][:2] + [{"jump": {"target": "x"}}]),
            "drop": lambda r: r.update(expr=r["expr"][:2] + [{"drop": None}]),
            "limit": lambda r: r["expr"].insert(2, {"limit": {"rate": 5, "per": "second"}}),
            "set of ifaces": lambda r: r["expr"][1]["match"].update(right={"set": [BRIDGE, "evil0"]}),
            "extra match": lambda r: r["expr"].insert(2, meta_match("mark", 1)),
            "comment": lambda r: r.update(comment="hi"),
            "family ip": lambda r: r.update(family="ip"),
            "nonlist expr": lambda r: r.update(expr={"accept": None}),
            "empty expr": lambda r: r.update(expr=[]),
            "accept with value": lambda r: r.update(expr=r["expr"][:2] + [{"accept": {}}]),
            "swapped order": lambda r: r["expr"].reverse(),
            "extra key": lambda r: r.update(index=0),
        }
        for name, fn in cases.items():
            with self.subTest(name):
                with self.assertRaises(ValueError):
                    guard(mutate(fn))

    def test_input_chain_rules_are_ignored(self):
        doc = ruleset()
        doc["nftables"].append(
            {"rule": {"family": "inet", "table": "bbraft_filter", "chain": "input", "handle": 99,
                      "expr": [{"jump": {"target": "x"}}, {"counter": {"packets": 1, "bytes": 2}}]}}
        )
        self.assertTrue(guard(doc))

    def test_unexpected_objects_in_the_target_table_are_refused(self):
        for name, extra in {
            "set": {"set": {"family": "inet", "table": "bbraft_filter", "name": "s", "type": "ipv4_addr", "handle": 5}},
            "map": {"map": {"family": "inet", "table": "bbraft_filter", "name": "m", "handle": 5}},
            "flowtable": {"flowtable": {"family": "inet", "table": "bbraft_filter", "name": "f", "handle": 5}},
            "counter": {"counter": {"family": "inet", "table": "bbraft_filter", "name": "c", "handle": 5}},
            "unknown": {"mystery": {"family": "inet", "table": "bbraft_filter"}},
        }.items():
            with self.subTest(name):
                doc = ruleset(extra=[extra])
                with self.assertRaises(ValueError):
                    guard(doc)

    def test_malformed_json_is_refused(self):
        for text in ["", "not json", "[]", "{}", '{"nftables": {}}', '{"nftables": [1]}',
                     '{"nftables": [{"a": 1, "b": 2}]}', '{"nftables": []} trailing', "null"]:
            with self.subTest(text):
                with self.assertRaises(ValueError):
                    guard(text)
        for bad in [None, 5, b"{}", ["x"]]:
            with self.subTest(repr(bad)):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_forward_guard(bad, [BRIDGE], BRIDGE, SSH)

    def test_duplicate_keys_are_refused(self):
        text = json.dumps(ruleset())
        dup = text.replace('"policy": "drop"', '"policy": "accept", "policy": "drop"', 1)
        self.assertNotEqual(text, dup)
        self.refused(dup, "duplicate")
        self.refused('{"nftables": [], "nftables": []}', "duplicate")

    def test_nonstandard_numbers_are_refused(self):
        text = json.dumps(ruleset()).replace('"handle": 10', '"handle": NaN', 1)
        self.refused(text, "")

    def test_oversized_output_is_refused(self):
        doc = ruleset()
        doc["nftables"].append({"metainfo": {"pad": "x" * (9 * 1024 * 1024)}})
        self.refused(doc, "too large")

    def test_bridge_config_is_validated_never_repaired(self):
        doc = ruleset()
        cases = {
            "empty": ([], BRIDGE, SSH),
            "string": (BRIDGE, BRIDGE, SSH),
            "bridge missing": (["other0"], BRIDGE, SSH),
            "ssh included": ([BRIDGE, SSH], BRIDGE, SSH),
            "duplicate": ([BRIDGE, BRIDGE], BRIDGE, SSH),
            "non string": ([BRIDGE, 5], BRIDGE, SSH),
            "empty name": ([BRIDGE, ""], BRIDGE, SSH),
            "bad name": ([BRIDGE, "bad/name"], BRIDGE, SSH),
            "reserved lo": ([BRIDGE, "lo"], BRIDGE, SSH),
            "none": (None, BRIDGE, SSH),
            "dict": ({BRIDGE: 1}, BRIDGE, SSH),
        }
        for name, (bridges, bridge, ssh) in cases.items():
            with self.subTest(name):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_forward_guard(json.dumps(doc), bridges, bridge, ssh)

    def test_configured_bridge_with_no_live_rules_is_refused(self):
        self.refused(ruleset(), "exactly 2", bridges=(BRIDGE, "br1"))

    def test_input_is_not_modified_by_the_filter_registration(self):
        self.assertIn("bbraft_node_forward_guard", FILTERS.FilterModule().filters())
        doc = ruleset()
        before = copy.deepcopy(doc)
        guard(doc)
        self.assertEqual(doc, before)


if __name__ == "__main__":
    unittest.main()
