"""Unit tests for the bbraft_node role's Jinja filters.

Run directly with python3; no Ansible or collection is needed:

    python3 roles/bbraft_node/tests/test_filters.py
"""

from __future__ import annotations

import importlib.util
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


class JailInputTest(unittest.TestCase):
    """A10: the jail flag text the role renders, mirroring cmd/bbraft-node."""

    def test_uid_range(self):
        for good in ["900000-900999", "1-2", "2147483646-2147483647"]:
            self.assertTrue(FILTERS.bbraft_node_uid_range_valid(good), good)
        for bad in ["", "900000", "0-99", "900999-900000", "5-5", "2147483000-2147483648",
                    "0900000-0900999", "+1-5", "1-5 ", " 1-5", "1--5", "a-b", "1-5-7", 7, None, True]:
            self.assertFalse(FILTERS.bbraft_node_uid_range_valid(bad), repr(bad))

    def test_block_device(self):
        for good in ["8:0", "253:0", "259:12", "4294967295:4294967295"]:
            self.assertTrue(FILTERS.bbraft_node_block_device_valid(good), good)
        for bad in ["", "8", "0:5", "sda", "8:", ":0", "08:0", "8:00", "4294967296:0", "8:0:1", " 8:0", 8, None]:
            self.assertFalse(FILTERS.bbraft_node_block_device_valid(bad), repr(bad))

    def test_path_ok(self):
        self.assertTrue(FILTERS.bbraft_node_path_ok("/usr/bin/qemu-img"))
        for bad in ["qemu-img", "/", "/usr/bin/../bin/qemu-img", "/a//b", "/a/", "/a,b", "/a\nb", 5, None]:
            self.assertFalse(FILTERS.bbraft_node_path_ok(bad), repr(bad))

    def test_rate(self):
        for good, want in [(0, 0), ("0", 0), (104857600, 104857600), ("2000", 2000), (2**64 - 1, 2**64 - 1)]:
            self.assertTrue(FILTERS.bbraft_node_rate_valid(good), repr(good))
            self.assertEqual(FILTERS.bbraft_node_rate_int(good), want)
        for bad in [-1, "-1", "fast", 1.5, "1.5", True, None, 2**64, " 5", "05"]:
            self.assertFalse(FILTERS.bbraft_node_rate_valid(bad), repr(bad))


class SystemdQuoteTest(unittest.TestCase):
    def test_plain_value(self):
        self.assertEqual(FILTERS.bbraft_node_systemd_exec_quote("all"), '"all"')

    def test_spaces_are_inside_one_argument(self):
        self.assertEqual(
            FILTERS.bbraft_node_systemd_exec_quote("/etc/bb raft/config"),
            '"/etc/bb raft/config"',
        )

    def test_exec_percent_dollar_quote_and_backslash_are_escaped(self):
        self.assertEqual(
            FILTERS.bbraft_node_systemd_exec_quote('a%b$c"d\\e'),
            '"a%%b$$c\\"d\\\\e"',
        )

    def test_unit_keeps_dollar_but_escapes_percent(self):
        self.assertEqual(
            FILTERS.bbraft_node_systemd_unit_quote('a%b$c"d\\e'),
            '"a%%b$c\\"d\\\\e"',
        )

    def test_path_value_is_never_quoted(self):
        # EnvironmentFile= and LoadCredential=<id>:<path> take the value
        # verbatim: a quoted path is "not absolute" to systemd, which ignores
        # the setting without failing the unit.
        value = FILTERS.bbraft_node_systemd_path_value
        self.assertEqual(value("/etc/bbraft/s3.env"), "/etc/bbraft/s3.env")
        self.assertEqual(value("/etc/bb raft/s3.env"), "/etc/bb raft/s3.env")
        self.assertEqual(value("/etc/a%b$c\"d\\e"), "/etc/a%%b$c\"d\\e")

    def test_path_value_refuses_what_systemd_would_misread(self):
        for bad in ["relative/s3.env", "", '"/etc/s3.env"', " /etc/s3.env", "/etc/s3.env ", "/etc/s3.env\\", "/etc/a\nb", "/etc/a\0b"]:
            with self.subTest(value=bad):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_systemd_path_value(bad)

    def test_line_breaks_are_rejected(self):
        for quote in (
            FILTERS.bbraft_node_systemd_exec_quote,
            FILTERS.bbraft_node_systemd_unit_quote,
        ):
            for value in ["a\nb", "a\rb", "a\0b"]:
                with self.subTest(quote=quote.__name__, value=value):
                    with self.assertRaises(ValueError):
                        quote(value)

    def test_empty_value_is_an_empty_argument(self):
        self.assertEqual(FILTERS.bbraft_node_systemd_exec_quote(""), '""')
        self.assertEqual(FILTERS.bbraft_node_systemd_unit_quote(""), '""')


class ServerAddrTest(unittest.TestCase):
    def test_accepts_loopback_and_private_literals(self):
        for addr in [
            "localhost:7701",
            "127.0.0.1:7701",
            "127.9.9.9:7701",
            "10.1.2.3:7701",
            "172.16.0.1:7701",
            "192.168.1.1:7701",
            "[::1]:7701",
            "[fc00::1]:7701",
            "[::ffff:127.0.0.1]:7701",
            "[::ffff:10.0.0.1]:7701",
        ]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_node_server_addr_valid(addr))

    def test_rejects_wildcard_public_link_local_and_hostnames(self):
        for addr in [
            "0.0.0.0:7701",
            ":7701",
            "[::]:7701",
            "203.0.113.10:7701",
            "[2001:db8::1]:7701",
            "169.254.1.1:7701",
            "[fe80::1]:7701",
            "example.com:7701",
        ]:
            with self.subTest(addr=addr):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_server_addr_valid(addr)

    def test_rejects_malformed_addresses(self):
        for addr in [
            "127.0.0.1",
            "127.0.0.1:0",
            "127.0.0.1:99999",
            "::1:7701",
            "[::1:7701",
            "[::1]7701",
            "127.0.0.1:77.5",
            "127.0.0.1: 7701",
            "127.0.0.1:\u0667\u0667\u0660\u0661",
        ]:
            with self.subTest(addr=addr):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_server_addr_valid(addr)


class PathTest(unittest.TestCase):
    def test_absolute_paths_are_accepted(self):
        for path in [
            "/var/lib/bbraft",
            "/etc/bbraft/node.token",
            "/tmp/a b/bb raft",
            "/usr/local/bin/bbraft-node",
        ]:
            with self.subTest(path=path):
                self.assertTrue(FILTERS.bbraft_node_path_valid(path))

    def test_relative_traversal_and_control_characters_are_rejected(self):
        for path in [
            "",
            "var/lib/bbraft",
            "./bbraft",
            "/var/lib/../etc",
            "/var/lib/bbraft/../../etc",
            "/var/lib/./bbraft",
            "/var/lib/bbraft/",
            "//var/lib/bbraft",
            "/var/lib//bbraft",
            "/var/lib/bbraft\nnode",
            "/var/lib/bbraft\0node",
        ]:
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_path_valid(path)


class ModeTest(unittest.TestCase):
    def test_mode_has_and_lacks(self):
        self.assertTrue(FILTERS.bbraft_node_mode_has("0555", 5))
        self.assertTrue(FILTERS.bbraft_node_mode_lacks("0555", 146))
        self.assertTrue(FILTERS.bbraft_node_mode_has("0600", 256))
        self.assertTrue(FILTERS.bbraft_node_mode_lacks("0600", 63))
        self.assertFalse(FILTERS.bbraft_node_mode_lacks("0644", 63))
        self.assertFalse(FILTERS.bbraft_node_mode_has("0500", 5))
        self.assertFalse(FILTERS.bbraft_node_mode_lacks("0666", 146))

    def test_bad_modes_are_rejected(self):
        for value in ["644", "0644 ", "0o644", "8888", "", 644]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_mode_has(value, 1)


class IntTypeTest(unittest.TestCase):
    def test_only_genuine_integers(self):
        for value in [0, 1, -1, 10737418240]:
            with self.subTest(value=value):
                self.assertTrue(FILTERS.bbraft_node_is_int(value))
        for value in [True, False, 1.0, "1", "", None, [1]]:
            with self.subTest(value=value):
                self.assertFalse(FILTERS.bbraft_node_is_int(value))


class ByteLengthTest(unittest.TestCase):
    def test_ascii_and_utf8(self):
        self.assertEqual(FILTERS.bbraft_node_byte_length("abc"), 3)
        self.assertEqual(FILTERS.bbraft_node_byte_length("é"), 2)
        self.assertEqual(FILTERS.bbraft_node_byte_length(""), 0)


class UintTest(unittest.TestCase):
    def test_accepts_zero_and_ascii_digits(self):
        self.assertEqual(FILTERS.bbraft_node_uint(0), 0)
        self.assertEqual(FILTERS.bbraft_node_uint("0"), 0)
        self.assertEqual(FILTERS.bbraft_node_uint(1024), 1024)
        self.assertEqual(FILTERS.bbraft_node_uint("4294967295"), 4294967295)

    def test_rejects_non_decimal_values(self):
        for value in [-1, 1.5, "1.5", "+1", " 1", "", True, False, "1e3", "\u0667"]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_uint(value)


class RatioTest(unittest.TestCase):
    def test_accepts_finite_positive_numbers(self):
        for value in [1, 2, 0.5, 1.0, 1.5, 2.25, "1", "1.0", "1.5", "0.25", "2e0", "1_0"]:
            with self.subTest(value=value):
                self.assertEqual(FILTERS.bbraft_node_ratio(value), value)

    def test_rejects_zero_negative_and_non_finite(self):
        for value in [
            0,
            0.0,
            -1,
            -0.5,
            "0",
            "-1.5",
            "nan",
            "NaN",
            "inf",
            "-inf",
            "Infinity",
            "1e309",
            "1e-400",
            float("nan"),
            float("inf"),
            float("-inf"),
        ]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_ratio(value)

    def test_rejects_booleans_garbage_and_lookalikes(self):
        for value in [
            True,
            False,
            None,
            [1],
            "",
            "abc",
            "1,5",
            "0x1p-2",
            " 1.5",
            "1.5 ",
            "1.5\n",
            "\u0667",
            "\uff11\uff12",
            10**400,
        ]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_ratio(value)


class DurationTest(unittest.TestCase):
    def test_accepts_positive_go_durations(self):
        for value in [
            "1ns",
            "300ms",
            "2m",
            "1h30m",
            "1.5h",
            "1h30m0.5s",
            "2562047h",
        ]:
            with self.subTest(value=value):
                self.assertEqual(FILTERS.bbraft_node_go_duration(value), value)

    def test_rejects_zero_sub_nanosecond_and_oversized_values(self):
        for value in [
            "0",
            "0s",
            "0.5ns",
            "9223372036854775808ns",
            "9223372036854775808.5ns",
            "2562048h",
        ]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_go_duration(value)

    def test_rejects_signs_bare_numbers_unknown_units_and_spaces(self):
        for value in ["-1s", "+1s", "1", "1d", "1e3s", " 2m", "2m ", "soon", ""]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_go_duration(value)


class IfaceNameTest(unittest.TestCase):
    def test_accepts_kernel_interface_names(self):
        for name in ["bbraft0", "eth0", "tailscale0", "br-abc_1", "a" * 15]:
            with self.subTest(name=name):
                self.assertTrue(FILTERS.bbraft_node_iface_name_valid(name))

    def test_rejects_empty_reserved_and_unsafe_names(self):
        for name in [
            "",
            "a" * 16,
            ".",
            "..",
            "-eth0",
            "lo",
            "bad name",
            "bad/name",
            "bad:name",
            'bad"name',
            "bad\\name",
            "bad$name",
            "bad\nname",
            "bad\0name",
        ]:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_iface_name_valid(name)


class AddressTest(unittest.TestCase):
    def test_ipv4_literals_only(self):
        for addr in ["10.0.0.1", "203.0.113.10", "0.0.0.0", "255.255.255.255"]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_node_ipv4_addr_valid(addr))
        for addr in ["::1", "::ffff:10.0.0.1", "10.0.0", "example.com", " 10.0.0.1", ""]:
            with self.subTest(addr=addr):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_ipv4_addr_valid(addr)

    def test_ipv4_prefixes(self):
        for prefix in ["10.42.0.0/16", "10.42.3.4/16", "192.168.0.0/24"]:
            with self.subTest(prefix=prefix):
                self.assertTrue(FILTERS.bbraft_node_ipv4_prefix_valid(prefix))
        for prefix in ["fd00::/64", "10.42.0.0", "10.42.0.0/33", "nope"]:
            with self.subTest(prefix=prefix):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_ipv4_prefix_valid(prefix)

    def test_cidr_suffix_is_a_decimal_length_only(self):
        # Python's ip_network also accepts dotted netmasks and hostmasks;
        # netip.ParsePrefix (the node's parser) does not, so the role must
        # reject them for every prefix input.
        for value in [
            "10.42.0.0/255.255.0.0",
            "10.42.0.0/0.0.255.255",
            "10.42.0.0/0x10",
            "10.42.0.0/ 16",
            "10.42.0.0/",
            "10.42.0.0",
        ]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_ipv4_prefix_valid(value)
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_guest_prefix_valid(value)

    def test_ingress_is_a_unicast_ipv4_address(self):
        for addr in [
            "10.1.2.3",
            "100.100.1.1",
            "203.0.113.10",
            "198.51.100.7",
            "192.0.2.9",
            "8.8.8.8",
        ]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_node_ingress_addr_valid(addr))
        for addr in [
            "127.0.0.1",
            "127.9.9.9",
            "0.0.0.0",
            "224.0.0.1",
            "239.1.1.1",
            "169.254.1.1",
            "255.255.255.255",
            "::1",
            "::ffff:10.0.0.1",
        ]:
            with self.subTest(addr=addr):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_ingress_addr_valid(addr)

    def test_guest_prefix_is_private_and_sized(self):
        for prefix in ["10.42.0.0/16", "10.42.3.4/16", "172.16.0.0/12", "192.168.0.0/24"]:
            with self.subTest(prefix=prefix):
                self.assertTrue(FILTERS.bbraft_node_guest_prefix_valid(prefix))
        for prefix in [
            "8.8.8.0/24",
            "100.64.0.0/10",
            "10.0.0.0/7",
            "10.0.0.0/31",
            "172.15.0.0/16",
        ]:
            with self.subTest(prefix=prefix):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_guest_prefix_valid(prefix)

    def test_resolvers_are_public_unicast(self):
        for addr in ["1.1.1.1", "9.9.9.9", "8.8.4.4"]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_node_resolver_valid(addr))
        for addr in [
            "10.1.1.1",
            "172.16.0.53",
            "192.168.0.53",
            "100.100.100.100",
            "127.0.0.1",
            "169.254.1.1",
            "224.0.0.1",
            "240.0.0.1",
            "0.0.0.0",
            "::1",
        ]:
            with self.subTest(addr=addr):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_resolver_valid(addr)

    def test_gateway_is_inside_and_off_the_edges(self):
        self.assertTrue(FILTERS.bbraft_node_gateway_valid("10.42.0.1", "10.42.0.0/16"))
        self.assertTrue(FILTERS.bbraft_node_gateway_valid("10.42.255.254", "10.42.0.0/16"))
        for gateway, prefix in [
            ("10.42.0.0", "10.42.0.0/16"),
            ("10.42.255.255", "10.42.0.0/16"),
            ("10.43.0.1", "10.42.0.0/16"),
            ("fd00::1", "10.42.0.0/16"),
            ("nope", "10.42.0.0/16"),
        ]:
            with self.subTest(gateway=gateway, prefix=prefix):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_gateway_valid(gateway, prefix)


class PrefixRelationTest(unittest.TestCase):
    def test_address_in_prefix(self):
        self.assertTrue(FILTERS.bbraft_node_addr_in_prefix("10.42.0.9", "10.42.0.0/16"))
        self.assertFalse(FILTERS.bbraft_node_addr_in_prefix("10.43.0.9", "10.42.0.0/16"))

    def test_address_in_any_prefix(self):
        self.assertTrue(
            FILTERS.bbraft_node_addr_in_prefixes("100.100.1.1", ["100.64.0.0/10"])
        )
        self.assertFalse(
            FILTERS.bbraft_node_addr_in_prefixes("1.1.1.1", ["100.64.0.0/10"])
        )
        self.assertFalse(FILTERS.bbraft_node_addr_in_prefixes("1.1.1.1", []))

    def test_prefix_overlap(self):
        self.assertTrue(
            FILTERS.bbraft_node_prefixes_overlap("10.42.0.0/24", "10.42.0.0/16")
        )
        self.assertFalse(
            FILTERS.bbraft_node_prefixes_overlap("10.43.0.0/24", "10.42.0.0/16")
        )


class SshPortRangeTest(unittest.TestCase):
    def test_accepts_publishable_inclusive_ranges(self):
        for value in ["22000-22999", "1024-65535", "1024-1024", "20300-20300"]:
            with self.subTest(value=value):
                self.assertTrue(FILTERS.bbraft_node_ssh_port_range_valid(value))

    def test_rejects_privileged_malformed_or_descending_ranges(self):
        for value in [
            "22-22999",
            "0-100",
            "22000",
            "22000-",
            "-22999",
            "22999-22000",
            "22000-65536",
            "1024 - 2048",
            "1024-2048x",
            " 1024-2048",
        ]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_node_ssh_port_range_valid(value)


if __name__ == "__main__":
    unittest.main()
