"""Unit tests for the role's Jinja filters.

Run directly with python3; no Ansible or collection is needed:

    python3 roles/bbraft_server/tests/test_filters.py
"""

from __future__ import annotations

import importlib.util
import pathlib
import unittest

MODULE_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "filter_plugins"
    / "bbraft_server_filters.py"
)
SPEC = importlib.util.spec_from_file_location("bbraft_server_filters", MODULE_PATH)
FILTERS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FILTERS)


class SystemdQuoteTest(unittest.TestCase):
    def test_plain_value(self):
        self.assertEqual(FILTERS.bbraft_systemd_exec_quote("all"), '"all"')

    def test_spaces_are_inside_one_argument(self):
        self.assertEqual(
            FILTERS.bbraft_systemd_exec_quote("/etc/bb raft/config"),
            '"/etc/bb raft/config"',
        )

    def test_exec_percent_dollar_quote_and_backslash_are_escaped(self):
        self.assertEqual(
            FILTERS.bbraft_systemd_exec_quote('a%b$c"d\\e'),
            '"a%%b$$c\\"d\\\\e"',
        )

    def test_unit_keeps_dollar_but_escapes_percent(self):
        self.assertEqual(
            FILTERS.bbraft_systemd_unit_quote('a%b$c"d\\e'),
            '"a%%b$c\\"d\\\\e"',
        )

    def test_line_breaks_are_rejected(self):
        for quote in (
            FILTERS.bbraft_systemd_exec_quote,
            FILTERS.bbraft_systemd_unit_quote,
        ):
            for value in ["a\nb", "a\rb", "a\0b"]:
                with self.subTest(quote=quote.__name__, value=value):
                    with self.assertRaises(ValueError):
                        quote(value)

    def test_empty_value_is_an_empty_argument(self):
        self.assertEqual(FILTERS.bbraft_systemd_exec_quote(""), '""')
        self.assertEqual(FILTERS.bbraft_systemd_unit_quote(""), '""')


class EffectiveKeyIDsTest(unittest.TestCase):
    def test_empty_id_defaults_to_owner(self):
        keys = [
            {"id": "main", "owner": "acct_1"},
            {"owner": "acct_2"},
        ]
        self.assertEqual(
            FILTERS.bbraft_effective_key_ids(keys), ["main", "acct_2"]
        )

    def test_default_id_collides_with_explicit_id(self):
        keys = [
            {"id": "acct", "owner": "other"},
            {"owner": "acct"},
        ]
        ids = FILTERS.bbraft_effective_key_ids(keys)
        self.assertEqual(ids, ["acct", "acct"])
        self.assertEqual(len(set(ids)), 1)


class BindTest(unittest.TestCase):
    def test_public(self):
        for addr in [
            "0.0.0.0:7701",
            ":7701",
            "203.0.113.10:7701",
            "[2001:db8::1]:7701",
        ]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_bind_is_public(addr))

    def test_not_public(self):
        for addr in [
            "localhost:7701",
            "127.0.0.1:7701",
            "10.1.2.3:7701",
            "172.16.0.1:7701",
            "192.168.1.1:7701",
            "169.254.1.1:7701",
            "[::1]:7701",
            "[fc00::1]:7701",
            "[fe80::1]:7701",
            "[::ffff:127.0.0.1]:7701",
            "[::ffff:10.0.0.1]:7701",
        ]:
            with self.subTest(addr=addr):
                self.assertFalse(FILTERS.bbraft_bind_is_public(addr))

    def test_ipv4_mapped_public(self):
        self.assertTrue(FILTERS.bbraft_bind_is_public("[::ffff:203.0.113.10]:7701"))

    def test_ipv4_mapped_loopback(self):
        self.assertTrue(FILTERS.bbraft_bind_is_loopback("[::ffff:127.0.0.1]:7702"))
        self.assertFalse(FILTERS.bbraft_bind_is_loopback("[::ffff:10.0.0.1]:7702"))

    def test_loopback(self):
        for addr in ["localhost:7702", "127.0.0.1:7702", "127.9.9.9:7702", "[::1]:7702"]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_bind_is_loopback(addr))
        for addr in ["0.0.0.0:7702", ":7702", "10.0.0.1:7702", "[2001:db8::1]:7702"]:
            with self.subTest(addr=addr):
                self.assertFalse(FILTERS.bbraft_bind_is_loopback(addr))

    def test_invalid_addresses_are_rejected(self):
        for addr in [
            "not-an-address",
            "127.0.0.1",
            "127.0.0.1:0",
            "127.0.0.1:99999",
            "example.com:7700",
            "::1:7701",              # unbracketed IPv6 is not host:port
            "[::1:7701",             # missing closing bracket
            "[::1]7701",             # missing colon after bracket
            "127.0.0.1:77.5",        # float port
            "127.0.0.1: 7701",       # space in port
            "127.0.0.1:\u0667\u0667\u0660\u0661",  # Arabic-Indic digits
        ]:
            with self.subTest(addr=addr):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_bind_is_public(addr)

    def test_listen_valid_accepts_the_same_syntax(self):
        for addr in [
            "127.0.0.1:7700",
            "localhost:7700",
            ":7700",
            "[::1]:7700",
            "[::ffff:127.0.0.1]:7700",
        ]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_listen_valid(addr))
        for addr in ["127.0.0.1", "::1:7700", "[::1]7700", "127.0.0.1:77.5"]:
            with self.subTest(addr=addr):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_listen_valid(addr)


class WildcardListenTest(unittest.TestCase):
    def test_wildcard_binds(self):
        for addr in ["0.0.0.0:7700", ":7700", "[::]:7700"]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_bind_is_wildcard(addr))

    def test_concrete_hosts_are_not_wildcard(self):
        for addr in [
            "127.0.0.1:7700",
            "localhost:7700",
            "[::1]:7700",
            "[::ffff:0.0.0.0]:7700",
            "203.0.113.10:7700",
        ]:
            with self.subTest(addr=addr):
                self.assertFalse(FILTERS.bbraft_bind_is_wildcard(addr))

    def test_malformed_addresses_are_rejected(self):
        for addr in ["not-an-address", "::1:7700", "0.0.0.0:port"]:
            with self.subTest(addr=addr):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_bind_is_wildcard(addr)


class PublicURLTest(unittest.TestCase):
    def test_accepts_absolute_http_and_https_base_urls(self):
        for url in [
            "",
            "http://bbraft.example.test",
            "https://bbraft.example.test",
            "https://bbraft.example.test/base/path",
            "https://example.test/a%20b",
            "https://127.0.0.1:7700",
            "http://localhost:7700/",
            "https://[::1]:7700",
            # net/url leaves an empty query/fragment out of RawQuery/Fragment.
            "https://bbraft.example.test?",
            "https://bbraft.example.test#",
        ]:
            with self.subTest(url=url):
                self.assertTrue(FILTERS.bbraft_public_url_valid(url))

    def test_rejects_malformed_urls_without_echoing_them(self):
        cases = [
            ("leading space", " https://example.test", "not a valid URL"),
            ("relative", "/api", "scheme"),
            ("non-http scheme", "ftp://example.test", "scheme"),
            ("no authority", "http:/path", "host"),
            ("empty host", "https://:443", "host"),
            ("empty authority", "http://", "host"),
            (
                "credentials",
                "https://user:password@example.test",
                "credentials",
            ),
            (
                "query",
                "https://example.test?token=placeholder-url-secret",
                "query",
            ),
            ("fragment", "https://example.test#fragment", "fragment"),
            ("signed port", "https://example.test:+7702", "not a valid URL"),
            ("host whitespace", "https://exa mple.test", "not a valid URL"),
            (
                "malformed path percent escape",
                "https://example.test/%zz",
                "not a valid URL",
            ),
            (
                "malformed host percent escape",
                "https://exa%zzmple.test",
                "not a valid URL",
            ),
            (
                "truncated path percent escape",
                "https://example.test/%2",
                "not a valid URL",
            ),
            (
                "malformed IPv6 authority",
                "https://[::1",
                "not a valid URL",
            ),
        ]
        for label, url, want in cases:
            with self.subTest(case=label):
                with self.assertRaises(ValueError) as caught:
                    FILTERS.bbraft_public_url_valid(url)
                message = str(caught.exception)
                self.assertIn(want, message)
                self.assertTrue(
                    url not in message,
                    "the validation error echoed the rejected URL",
                )
                self.assertTrue(
                    caught.exception.__cause__ is None,
                    "the parser error must stay detached from the raised error",
                )

    def test_out_of_range_url_port_is_still_valid_like_net_url(self):
        # publicURL only checks the scheme/host; net/url accepts any numeric
        # port and the binary has no range rule for the advertised URL.
        self.assertTrue(
            FILTERS.bbraft_public_url_valid("https://example.test:99999")
        )


class DialTest(unittest.TestCase):
    def test_valid_dial_targets(self):
        for addr in [
            "",
            "127.0.0.1:7702",
            "localhost:7702",
            "gateway.internal:9702",
            "[::1]:7702",
            "[::ffff:127.0.0.1]:9702",
            "127.0.0.1:07702",
        ]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_dial_valid(addr))

    def test_invalid_dial_targets(self):
        for addr in [
            ":7702",
            "127.0.0.1",
            "127.0.0.1:0",
            "127.0.0.1:65536",
            "127.0.0.1:+7702",
            "127.0.0.1:77.5",
            "127.0.0.1: 7702",
            "127.0.0.1:\u0667\u0667\u0660\u0662",
            "::1:7702",
            "[::1]:",
            "gateway.internal:port",
        ]:
            with self.subTest(addr=addr):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_dial_valid(addr)

    def test_loopback_dial_targets(self):
        for addr in [
            "127.0.0.1:7702",
            "127.9.9.9:7702",
            "localhost:7702",
            "[::1]:7702",
            "[::ffff:127.0.0.1]:7702",
        ]:
            with self.subTest(addr=addr):
                self.assertTrue(FILTERS.bbraft_dial_is_loopback(addr))

    def test_non_loopback_dial_targets(self):
        for addr in [
            "",
            "10.0.0.5:7702",
            "gateway.internal:9702",
            "[2001:db8::1]:7702",
            "[::ffff:10.0.0.1]:7702",
            "::1:7702",
        ]:
            with self.subTest(addr=addr):
                self.assertFalse(FILTERS.bbraft_dial_is_loopback(addr))

    def test_default_dial_maps_wildcard_binds_to_loopback(self):
        for listen in ["0.0.0.0:7702", ":7702", "[::]:7702"]:
            with self.subTest(listen=listen):
                self.assertEqual(
                    FILTERS.bbraft_gateway_dial(listen), "127.0.0.1:7702"
                )

    def test_default_dial_keeps_other_listen_hosts(self):
        for listen, want in [
            ("127.0.0.1:7702", "127.0.0.1:7702"),
            ("localhost:7702", "localhost:7702"),
            ("[::1]:7702", "[::1]:7702"),
            ("[::ffff:127.0.0.1]:7702", "[::ffff:127.0.0.1]:7702"),
        ]:
            with self.subTest(listen=listen):
                self.assertEqual(FILTERS.bbraft_gateway_dial(listen), want)


class GatewayServerNameTest(unittest.TestCase):
    def test_accepts_empty_and_nonspace_names(self):
        for name in [
            "",
            "gateway.internal",
            "bbraft.example.test",
            "gateway-1",
            "gw\u00fcnstig.example",
            "\x1cgw",  # Python isspace() is true, Go's unicode.IsSpace is not
        ]:
            with self.subTest(name=name):
                self.assertTrue(FILTERS.bbraft_gateway_server_name_valid(name))

    def test_rejects_whitespace_and_nul_without_echoing_the_value(self):
        for name in [
            "gateway internal",
            "gateway\tinternal",
            "gateway\ninternal",
            "gateway\rinternal",
            "\u00a0gateway",
            "\u3000gateway",
            "\u2028gateway",
            "\u2029gateway",
            "gateway\0internal",
        ]:
            with self.subTest(name=name):
                with self.assertRaises(ValueError) as caught:
                    FILTERS.bbraft_gateway_server_name_valid(name)
                self.assertNotIn(name, str(caught.exception))


class PositiveIntTest(unittest.TestCase):
    def test_accepts_ints_and_ascii_digit_strings(self):
        self.assertEqual(FILTERS.bbraft_positive_int(7200), 7200)
        self.assertEqual(FILTERS.bbraft_positive_int("7200"), 7200)

    def test_rejects_floats_and_non_decimal_strings(self):
        for value in [1.5, "1.5", "7200x", "1e3", " 7200", "0", 0, -1, True]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    FILTERS.bbraft_positive_int(value)

    def test_rejects_unicode_digits(self):
        with self.assertRaises(ValueError):
            FILTERS.bbraft_positive_int("\u0667\u0662\u0660\u0660")


if __name__ == "__main__":
    unittest.main()
