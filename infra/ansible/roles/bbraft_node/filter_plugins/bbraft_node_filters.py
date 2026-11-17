"""Jinja filters for the bbraft_node role.

Only the Python standard library is used, so the filters also load in the
role-local unit test without Ansible. They keep the systemd quoting, the
phase-0.5 server-address rule and the scalar checks in one place, mirroring
``cmd/bbraft-node/config.go``:

* ``bbraft_node_server_addr_valid`` accepts exactly what ``checkServerAddr``
  accepts: ``host:port`` with an ASCII-decimal port 1-65535, ``localhost`` or
  a literal loopback/private (RFC 1918 / ULA) IP. Wildcard, link-local and
  public addresses are rejected because the phase 0.5 link is plaintext.
* ``bbraft_node_systemd_exec_quote`` produces one ExecStart argument. It
  rejects line breaks and NUL and escapes ``%``, ``$``, ``"`` and ``\\`` the
  way systemd expects.
* ``bbraft_node_systemd_unit_quote`` does the same for settings such as
  ``ReadWritePaths=``, where ``$`` is literal.
* ``bbraft_node_uint`` and ``bbraft_node_go_duration`` reject values the Go
  flag package would truncate or reject, so the rendered argv is exactly the
  validated value.
"""

from __future__ import annotations

import ipaddress
import json
import math
import posixpath
import re

# Go's netip.IsPrivate is RFC 1918 plus fc00::/7. Python's ipaddress
# is_private also covers link-local and other special ranges, so list the
# exact networks instead of using it.
_PRIVATE_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("fc00::/7"),
)

_DURATION_TOKEN = re.compile(r"(\d+)(?:\.(\d+))?(ns|us|µs|μs|ms|s|m|h)")
_DURATION_UNIT_NS = {
    "ns": 1,
    "us": 1000,
    "µs": 1000,
    "μs": 1000,
    "ms": 1000 * 1000,
    "s": 1000 * 1000 * 1000,
    "m": 60 * 1000 * 1000 * 1000,
    "h": 60 * 60 * 1000 * 1000 * 1000,
}
_MAX_INT64 = (1 << 63) - 1


def _split_host(addr):
    """Return the host part of a host:port value.

    Mirrors Go's net.SplitHostPort: IPv6 must be bracketed, so an unbracketed
    ``::1:7701`` is malformed rather than an IPv6 host with a port. The port
    must be ASCII decimal digits in 1-65535; Python's isdigit() also accepts
    Unicode digits, which Go's strconv.Atoi rejects.
    """
    text = str(addr)
    if text.startswith("["):
        end = text.find("]")
        if end < 0:
            raise ValueError(f"server.addr {text!r}: missing closing ]")
        host = text[1:end]
        rest = text[end + 1 :]
        if not rest.startswith(":"):
            raise ValueError(f"server.addr {text!r} is not [host]:port")
        port = rest[1:]
    else:
        host, sep, port = text.rpartition(":")
        if not sep:
            raise ValueError(f"server.addr {text!r} is not host:port")
        if ":" in host:
            raise ValueError(
                f"server.addr {text!r}: IPv6 addresses must be bracketed"
            )
    if not port or any(ch not in "0123456789" for ch in port):
        raise ValueError(f"server.addr {text!r}: port must be ASCII digits")
    if not 1 <= int(port) <= 65535:
        raise ValueError(f"server.addr {text!r} has a port outside 1-65535")
    return host


def _parse_ip(host, addr):
    try:
        ip = ipaddress.ip_address(host)
    except ValueError as err:
        raise ValueError(
            f"server.addr {addr!r}: host must be localhost or an IP literal"
        ) from err
    # netip treats an IPv4-mapped IPv6 address as its IPv4 address for
    # IsLoopback/IsPrivate. Python does not, so unmap.
    mapped = getattr(ip, "ipv4_mapped", None)
    return mapped if mapped is not None else ip


def bbraft_node_server_addr_valid(addr):
    """Return True for a server address the node's checkServerAddr accepts."""
    host = _split_host(addr)
    if host == "":
        raise ValueError(
            f"server.addr {addr!r}: a host is required; the wildcard address is not usable"
        )
    if host.lower() == "localhost":
        return True
    ip = _parse_ip(host, addr)
    if ip.is_unspecified:
        raise ValueError(
            f"server.addr {addr!r}: the wildcard address is not usable"
        )
    if not ip.is_loopback and not any(ip in network for network in _PRIVATE_NETWORKS):
        raise ValueError(
            f"server.addr {addr!r}: host must be loopback or private; "
            "plaintext links may not leave a private network"
        )
    return True


def bbraft_node_path_valid(path):
    """Return True for a clean canonical absolute POSIX path.

    ``posixpath.normpath`` equality rejects aliases such as ``/var/lib/./x``,
    repeated slashes and trailing slashes, so string-only equality and prefix
    comparisons between destinations cannot miss the same actual path. The
    path is lexical only: the role never resolves remote symlinks.
    """
    text = str(path)
    if not text.startswith("/"):
        raise ValueError(f"path {text!r} is not absolute")
    if any(ch in text for ch in ("\n", "\r", "\0")):
        raise ValueError(f"path {text!r} contains a line break or NUL")
    if ".." in text.split("/"):
        raise ValueError(f"path {text!r} contains a '..' component")
    if "//" in text:
        raise ValueError(f"path {text!r} contains repeated slashes")
    if posixpath.normpath(text) != text:
        raise ValueError(f"path {text!r} is not a clean canonical path")
    return True


# Guest networking filters. They mirror nodenet.Config.Validate and
# cmd/bbraft-node/config.go so a role run rejects the same values the binary
# would reject at startup, and render exactly the validated value.

# IFNAMSIZ includes the terminating NUL, so 15 bytes is the maximum.
_MAX_IFACE_NAME_BYTES = 15
_IFACE_FORBIDDEN = frozenset("/:\\\"'` $|&;()<>{}[]*?\x00\n\r\t ")

# privateGuestRanges are the only ranges a guest prefix may come from.
_PRIVATE_GUEST_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)

# builtinBlocked mirrors nodenet's no-guest-destination set, so a resolver
# inside it is not a usable public resolver.
_BUILTIN_BLOCKED = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
)

_PORT_RANGE = re.compile(r"(\d{1,5})-(\d{1,5})\Z", re.ASCII)


def bbraft_node_iface_name_valid(name):
    """Return True for an interface name the kernel and nodenet accept."""
    text = str(name)
    if not text:
        raise ValueError("interface name is empty")
    if len(text.encode("utf-8")) > _MAX_IFACE_NAME_BYTES:
        raise ValueError(
            f"interface name {text!r} is longer than {_MAX_IFACE_NAME_BYTES} bytes"
        )
    if text in (".", ".."):
        raise ValueError(f"{text!r} is not an interface name")
    if text.startswith("-"):
        raise ValueError(f"{text!r} starts with a dash")
    for ch in text:
        if ord(ch) <= 0x1F or ord(ch) == 0x7F:
            raise ValueError(f"{text!r} contains a control character")
    if any(ch in _IFACE_FORBIDDEN for ch in text):
        raise ValueError(f"{text!r} contains a forbidden character")
    if text == "lo":
        raise ValueError('"lo" is reserved')
    return True


def _ipv4(value):
    """Parse a literal IPv4 address, rejecting IPv6 and 4-in-6 forms."""
    text = str(value)
    try:
        ip = ipaddress.ip_address(text)
    except ValueError as err:
        raise ValueError(f"{text!r} is not an IPv4 address") from err
    if not isinstance(ip, ipaddress.IPv4Address):
        raise ValueError(f"{text!r} is not an IPv4 address")
    return ip


def _ipv4_prefix(value):
    text = str(value)
    address, sep, bits = text.rpartition("/")
    # netip.ParsePrefix, unlike Python's ip_network, accepts only a decimal
    # length; reject dotted netmasks and hostmasks instead of masking them.
    if (
        not sep
        or not address
        or not bits
        or any(ch not in "0123456789" for ch in bits)
    ):
        raise ValueError(f"{text!r} is not an IPv4 prefix")
    try:
        network = ipaddress.ip_network(text, strict=False)
    except ValueError as err:
        raise ValueError(f"{text!r} is not an IPv4 prefix") from err
    if not isinstance(network, ipaddress.IPv4Network):
        raise ValueError(f"{text!r} is not an IPv4 prefix")
    return network


def bbraft_node_ipv4_addr_valid(addr):
    """Return True for a literal IPv4 address (no IPv6 or 4-in-6 form)."""
    _ipv4(addr)
    return True


def bbraft_node_ipv4_prefix_valid(prefix):
    """Return True for a literal IPv4 prefix, host bits ignored."""
    _ipv4_prefix(prefix)
    return True


def bbraft_node_ingress_addr_valid(addr):
    """Return True for a unicast IPv4 ingress address.

    Mirrors nodenet's validateIngress, which requires ``IsGlobalUnicast``:
    private and CGNAT addresses are allowed (a tailnet ingress is the normal
    first deployment), while loopback, unspecified, multicast, link-local and
    the limited broadcast address are refused.
    """
    ip = _ipv4(addr)
    if ip.is_unspecified or ip.is_loopback or ip.is_multicast or ip.is_link_local:
        raise ValueError(f"ingress address {ip} is not a unicast address")
    if str(ip) == "255.255.255.255":
        raise ValueError(f"ingress address {ip} is not a unicast address")
    return True


def bbraft_node_guest_prefix_valid(prefix):
    """Return True for a nodenet guest range: /8 to /30, wholly private."""
    network = _ipv4_prefix(prefix)
    if not 8 <= network.prefixlen <= 30:
        raise ValueError(f"guest prefix {network} must be between /8 and /30")
    if not any(network.subnet_of(r) for r in _PRIVATE_GUEST_NETWORKS):
        raise ValueError(
            f"guest prefix {network} is not a private range "
            "(10.0.0.0/8, 172.16.0.0/12 or 192.168.0.0/16)"
        )
    return True


def bbraft_node_resolver_valid(addr):
    """Return True for a public unicast IPv4 resolver."""
    ip = _ipv4(addr)
    if ip.is_unspecified or ip.is_multicast or ip.is_link_local:
        raise ValueError(f"resolver {ip} is not a public unicast address")
    if any(ip in network for network in _BUILTIN_BLOCKED):
        raise ValueError(f"resolver {ip} is not a public unicast address")
    return True


def bbraft_node_gateway_valid(gateway, prefix):
    """Return True for a gateway inside the prefix and off its edges."""
    ip = _ipv4(gateway)
    network = _ipv4_prefix(prefix)
    if ip not in network:
        raise ValueError(f"gateway {ip} is outside {network}")
    if ip == network.network_address:
        raise ValueError(f"gateway {ip} is the network address of {network}")
    if ip == network.broadcast_address:
        raise ValueError(f"gateway {ip} is the broadcast address of {network}")
    return True


def bbraft_node_addr_in_prefix(addr, prefix):
    """Return True when the IPv4 address is inside the prefix."""
    return _ipv4(addr) in _ipv4_prefix(prefix)


def bbraft_node_addr_in_prefixes(addr, prefixes):
    """Return True when the IPv4 address is inside any prefix of the list."""
    ip = _ipv4(addr)
    return any(ip in _ipv4_prefix(p) for p in prefixes)


def bbraft_node_prefixes_overlap(a, b):
    """Return True when two IPv4 prefixes share any address."""
    return _ipv4_prefix(a).overlaps(_ipv4_prefix(b))


def bbraft_node_ssh_port_range_valid(value):
    """Return True for an inclusive min-max port pool the node can publish."""
    text = str(value)
    match = _PORT_RANGE.fullmatch(text)
    if not match:
        raise ValueError(f"{text!r} is not an inclusive min-max port range")
    minimum, maximum = int(match.group(1)), int(match.group(2))
    if minimum < 1024:
        raise ValueError(
            f"SSH ingress ports {text!r} must start at or above 1024"
        )
    if maximum > 65535:
        raise ValueError(f"SSH ingress ports {text!r} must end at or below 65535")
    if minimum > maximum:
        raise ValueError(
            f"SSH ingress ports {text!r}: minimum {minimum} is above maximum {maximum}"
        )
    return True


def bbraft_node_uint(value):
    """Return a non-negative decimal integer without truncating input.

    YAML integers and ASCII-decimal strings are accepted. Floats, bools,
    strings with a sign/fraction/spaces and non-ASCII digits are rejected
    instead of being truncated by Jinja's ``| int`` filter.
    """
    if isinstance(value, bool):
        raise ValueError(f"expected a non-negative integer, got boolean {value!r}")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value and all(ch in "0123456789" for ch in value):
        number = int(value)
    else:
        raise ValueError(f"expected a non-negative decimal integer, got {value!r}")
    if number < 0:
        raise ValueError(f"expected a non-negative integer, got {number}")
    return number


def bbraft_node_go_duration(value):
    """Validate a positive Go duration string and return it unchanged.

    Accepts the units Go accepts (``ns``, ``us``, ``µs``, ``μs``, ``ms``, ``s``,
    ``m``, ``h``) with compound and fractional forms such as ``1h30m`` and
    ``1.5h``. The arithmetic is exact integer nanoseconds (no float rounding)
    and the total must fit Go's int64 duration, so an oversized value cannot
    reach ``flag.Duration`` as a silently wrapped number. Zero, sub-nanosecond
    totals, signs, bare numbers and unknown units are rejected.
    """
    text = str(value)
    if not text or text[0] in "+-":
        raise ValueError(f"duration {text!r} must be positive")
    pos = 0
    total = 0
    for match in _DURATION_TOKEN.finditer(text):
        if match.start() != pos:
            raise ValueError(f"duration {text!r} is not a Go duration")
        pos = match.end()
        whole = int(match.group(1))
        fraction = match.group(2) or ""
        unit = _DURATION_UNIT_NS[match.group(3)]
        if whole > _MAX_INT64 // unit:
            raise ValueError(f"duration {text!r} overflows int64 nanoseconds")
        nanos = whole * unit
        if fraction:
            divisor = 10 ** len(fraction)
            nanos += int(fraction) * unit // divisor
        if nanos > _MAX_INT64 - total:
            raise ValueError(f"duration {text!r} overflows int64 nanoseconds")
        total += nanos
    if pos != len(text) or total <= 0:
        raise ValueError(f"duration {text!r} must be a positive Go duration")
    return text


def bbraft_node_ratio(value):
    """Return a finite positive overcommit ratio unchanged.

    The node's ``flag.Float64`` overcommit flags multiply a detected host
    resource, so the value must be a genuine number: YAML ints and floats and
    decimal strings (including exponent and Go-underscore forms the parser
    also accepts) are validated; booleans, ``NaN``/``Inf``, zero, negatives,
    non-ASCII digits, surrounding whitespace and arbitrary strings are
    rejected instead of being swallowed by Jinja's permissive ``| float``.
    Values that overflow to infinity or underflow to zero fail the same
    bounds Go's ``strconv.ParseFloat`` enforces.
    """
    if isinstance(value, bool):
        raise ValueError(f"expected a finite positive ratio, got boolean {value!r}")
    if isinstance(value, (int, float)):
        try:
            number = float(value)
        except OverflowError as err:
            raise ValueError(f"expected a finite positive ratio, got {value!r}") from err
        result = value
    elif isinstance(value, str):
        if not value.isascii():
            raise ValueError(f"expected a finite positive ratio, got non-ASCII {value!r}")
        if value != value.strip():
            raise ValueError(f"expected a finite positive ratio, got padded {value!r}")
        try:
            number = float(value)
        except ValueError as err:
            raise ValueError(f"expected a finite positive ratio, got {value!r}") from err
        result = value
    else:
        raise ValueError(f"expected a finite positive ratio, got {value!r}")
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"expected a finite positive ratio, got {value!r}")
    return result


def bbraft_node_is_int(value):
    """Return True only for a genuine integer, excluding bool.

    Manifest numbers come from JSON, where ``"1"`` is a string and ``true`` is
    a bool; both fail unmarshalling into the node's integer struct fields.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def _mode_int(value):
    """Parse a four-digit octal mode string into an int."""
    text = str(value)
    if len(text) != 4 or any(ch not in "01234567" for ch in text):
        raise ValueError(f"mode {text!r} is not four octal digits")
    return int(text, 8)


def bbraft_node_mode_has(value, mask):
    """Return True when every bit in ``mask`` is set in the octal mode."""
    return (_mode_int(value) & int(mask)) == int(mask)


def bbraft_node_mode_lacks(value, mask):
    """Return True when no bit in ``mask`` is set in the octal mode."""
    return (_mode_int(value) & int(mask)) == 0


def bbraft_node_byte_length(value):
    """Return the UTF-8 byte length of a string, as the node's token bound is bytes."""
    text = str(value)
    try:
        return len(text.encode("utf-8"))
    except UnicodeEncodeError as err:
        raise ValueError(f"value is not valid UTF-8: {err}") from err


def bbraft_node_systemd_exec_quote(value):
    """Quote one value as a systemd ExecStart argument.

    systemd expands ``%`` specifiers and ``$VAR``/``${VAR}`` environment
    references in ``ExecStart``; ``%%`` and ``$$`` produce the literals. A
    line break cannot appear in an ExecStart line, so it is rejected instead
    of silently truncating the command.
    """
    text = str(value)
    if "\n" in text or "\r" in text or "\0" in text:
        raise ValueError(f"value contains a line break or NUL: {text!r}")
    escaped = (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("%", "%%")
        .replace("$", "$$")
    )
    return f'"{escaped}"'


def bbraft_node_systemd_unit_quote(value):
    """Quote one value for a non-ExecStart systemd setting.

    Settings such as ``ReadWritePaths=`` do not perform environment-variable
    substitution, so a literal ``$`` must stay a single ``$``. Specifier
    expansion still applies, so ``%`` becomes ``%%``.
    """
    text = str(value)
    if "\n" in text or "\r" in text or "\0" in text:
        raise ValueError(f"value contains a line break or NUL: {text!r}")
    escaped = text.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    return f'"{escaped}"'


def bbraft_node_path_ok(path):
    """Like bbraft_node_path_valid but report False instead of raising, so a
    caller's own message names the input (jail paths). A comma is refused too:
    the jail list flags are comma-separated."""
    if not isinstance(path, str) or "," in path or path == "/":
        return False
    try:
        return bbraft_node_path_valid(path)
    except ValueError:
        return False


def _canonical_uint(text, limit):
    """Return int(text) for canonical ASCII decimal text below limit, else None."""
    if not isinstance(text, str) or not text or not all(ch in "0123456789" for ch in text):
        return None
    if len(text) > 1 and text[0] == "0":
        return None
    number = int(text)
    return number if number < limit else None


def bbraft_node_uid_range_valid(value):
    """Report whether value is the jail uid range text "min-max".

    Mirrors cmd/bbraft-node parseUIDRange (0 < min < max < 2^31) and also
    requires canonical decimals, so the rendered flag is exactly what was
    validated.
    """
    if not isinstance(value, str) or value.count("-") != 1:
        return False
    low, high = value.split("-")
    minimum, maximum = _canonical_uint(low, 2**31), _canonical_uint(high, 2**31)
    return minimum is not None and maximum is not None and 0 < minimum < maximum


def bbraft_node_block_device_valid(value):
    """Report whether value is canonical "MAJ:MIN" with a nonzero uint32 major
    (cmd/bbraft-node parseBlockDevice)."""
    if not isinstance(value, str) or value.count(":") != 1:
        return False
    major, minor = value.split(":")
    major_n, minor_n = _canonical_uint(major, 2**32), _canonical_uint(minor, 2**32)
    return major_n is not None and minor_n is not None and major_n > 0


def _rate(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 <= value < 2**64 else None
    return _canonical_uint(value, 2**64)


def bbraft_node_rate_valid(value):
    """Report whether value is a jail IO rate: a uint64 as a YAML integer or
    canonical decimal text (0 omits the flag)."""
    return _rate(value) is not None


def bbraft_node_rate_int(value):
    """Return the validated jail IO rate as an int."""
    number = _rate(value)
    if number is None:
        raise ValueError(f"expected a uint64 IO rate, got {value!r}")
    return number


# Forward guard: the role persists and applies net.ipv4.ip_forward=1 only when
# the live ruleset proves the hardening role's forward filter is in place.

_NFT_MAX_BYTES = 8 * 1024 * 1024
_GUARD_TABLE = "bbraft_filter"
_GUARD_FAMILY = "inet"
_CHAIN_KEYS = frozenset(
    {"family", "table", "name", "handle", "type", "hook", "prio", "policy"}
)
_RULE_KEYS = frozenset({"family", "table", "chain", "handle", "expr"})


def _no_duplicate_keys(pairs):
    seen = {}
    for key, value in pairs:
        if key in seen:
            raise ValueError(f"nft JSON has a duplicate key {key!r}")
        seen[key] = value
    return seen


def _no_constant(name):
    raise ValueError(f"nft JSON has a non-standard number {name}")


def _guard_bridges(bridges, bridge, ssh_iface):
    """Validate the configured bridge list; never repair or deduplicate it."""
    if not isinstance(bridges, (list, tuple)) or not bridges:
        raise ValueError("hardening_vm_bridge_interfaces must be a nonempty list")
    for name in bridges:
        if not isinstance(name, str):
            raise ValueError(f"hardening_vm_bridge_interfaces entry {name!r} is not a string")
        bbraft_node_iface_name_valid(name)
    if len(set(bridges)) != len(bridges):
        raise ValueError("hardening_vm_bridge_interfaces has duplicate entries")
    if bridge not in bridges:
        raise ValueError(
            f"hardening_vm_bridge_interfaces does not contain the node bridge {bridge!r}"
        )
    if ssh_iface in bridges:
        raise ValueError(
            f"hardening_vm_bridge_interfaces contains the ssh ingress interface {ssh_iface!r}"
        )
    return list(bridges)


def _guard_meta_match(expr, key):
    """Return the right-hand string of ``meta <key> == <string>``, else None."""
    if not isinstance(expr, dict) or list(expr) != ["match"]:
        return None
    match = expr["match"]
    if not isinstance(match, dict) or set(match) != {"op", "left", "right"}:
        return None
    if match["op"] != "==" or match["left"] != {"meta": {"key": key}}:
        return None
    right = match["right"]
    return right if isinstance(right, str) else None


def _guard_forward_rule(rule, bridges):
    """Return (direction, bridge) for one exact IPv4 bridge accept rule."""
    if not isinstance(rule, dict) or not set(rule) <= _RULE_KEYS:
        raise ValueError("a forward rule has unexpected keys")
    expr = rule.get("expr")
    if (
        rule.get("family") != _GUARD_FAMILY
        or not isinstance(expr, list)
        or len(expr) != 3
        or expr[2] != {"accept": None}
        or _guard_meta_match(expr[0], "nfproto") != "ipv4"
    ):
        raise ValueError(
            "the forward chain has a rule that is not exactly "
            "'meta nfproto ipv4 iifname|oifname <bridge> accept'"
        )
    for direction in ("iifname", "oifname"):
        name = _guard_meta_match(expr[1], direction)
        if name is not None:
            if name not in bridges:
                raise ValueError(f"the forward chain accepts unconfigured interface {name!r}")
            return direction, name
    raise ValueError(
        "the forward chain has a rule that is not exactly "
        "'meta nfproto ipv4 iifname|oifname <bridge> accept'"
    )


def bbraft_node_forward_guard(nft_json, bridges, bridge, ssh_iface):
    """Return True when live ``nft -j list ruleset`` output proves the forward
    filter the role needs before it enables IPv4 forwarding.

    The ruleset must have exactly one ``inet bbraft_filter`` table whose only
    forward-hook chain is the base chain ``forward`` (type filter, priority 0,
    policy drop). Every rule in it must be exactly ``meta nfproto ipv4
    iifname|oifname <bridge> accept`` and each configured bridge must have
    exactly one rule per direction. Input-chain rules, other tables and other
    families are not inspected. Sets, maps, other objects of the table, extra
    expressions, IPv6, unknown keys, duplicate JSON keys and oversized output
    are refused. ``bridges`` is validated as configured (nonempty, unique,
    valid names, containing ``bridge`` and not ``ssh_iface``), never repaired.
    """
    configured = _guard_bridges(bridges, bridge, ssh_iface)
    if not isinstance(nft_json, str):
        raise ValueError("the nft output is not text")
    if len(nft_json.encode("utf-8", "surrogatepass")) > _NFT_MAX_BYTES:
        raise ValueError(f"the nft output is too large (over {_NFT_MAX_BYTES} bytes)")
    try:
        doc = json.loads(
            nft_json,
            object_pairs_hook=_no_duplicate_keys,
            parse_constant=_no_constant,
        )
    except RecursionError as err:
        raise ValueError("the nft output is nested too deeply") from err
    except ValueError as err:
        raise ValueError(f"the nft output is not valid JSON: {err}") from err
    if not isinstance(doc, dict) or set(doc) != {"nftables"} or not isinstance(doc["nftables"], list):
        raise ValueError("the nft output is not an nftables object list")

    tables = 0
    forward_chains = []
    forward_hook_chains = 0
    forward_rules = []
    for item in doc["nftables"]:
        if not isinstance(item, dict) or len(item) != 1:
            raise ValueError("the nft output has a malformed list entry")
        (kind, body), = item.items()
        if not isinstance(body, dict):
            raise ValueError(f"the nft {kind} entry is malformed")
        if kind == "metainfo":
            continue
        if body.get("family") != _GUARD_FAMILY:
            continue
        if kind == "table":
            if body.get("name") != _GUARD_TABLE:
                continue
            if not set(body) <= {"family", "name", "handle"}:
                raise ValueError("the inet bbraft_filter table has unexpected attributes")
            tables += 1
            continue
        if body.get("table") != _GUARD_TABLE:
            continue
        if kind == "chain":
            if body.get("hook") == "forward":
                forward_hook_chains += 1
            if body.get("name") == "forward" and body.get("hook") == "forward":
                forward_chains.append(body)
        elif kind == "rule":
            if body.get("chain") == "forward":
                forward_rules.append(body)
        else:
            raise ValueError(
                f"the inet bbraft_filter table has an unexpected {kind} object"
            )

    if tables > 1 or forward_hook_chains > 1:
        raise ValueError(
            "more than one inet bbraft_filter table or forward-hook chain is loaded"
        )
    if tables != 1 or len(forward_chains) != 1:
        raise ValueError("there is no inet bbraft_filter forward chain loaded")
    chain = forward_chains[0]
    if not set(chain) <= _CHAIN_KEYS:
        raise ValueError("the forward chain has unexpected attributes")
    if chain.get("type") != "filter":
        raise ValueError("the forward chain type is not filter")
    prio = chain.get("prio")
    if isinstance(prio, bool) or prio != 0:
        raise ValueError("the forward chain priority is not 0")
    if chain.get("policy") != "drop":
        raise ValueError("the forward chain policy is not drop")

    counts = {}
    for rule in forward_rules:
        key = _guard_forward_rule(rule, configured)
        counts[key] = counts.get(key, 0) + 1
    for name in configured:
        for direction in ("iifname", "oifname"):
            if counts.get((direction, name)) != 1:
                raise ValueError(
                    f"the forward chain must have exactly 2 rules (iifname and oifname) "
                    f"for {name!r}, one each"
                )
    return True


class FilterModule:
    """Expose the filters to Jinja."""

    def filters(self):
        return {
            "bbraft_node_server_addr_valid": bbraft_node_server_addr_valid,
            "bbraft_node_path_valid": bbraft_node_path_valid,
            "bbraft_node_iface_name_valid": bbraft_node_iface_name_valid,
            "bbraft_node_ipv4_addr_valid": bbraft_node_ipv4_addr_valid,
            "bbraft_node_ipv4_prefix_valid": bbraft_node_ipv4_prefix_valid,
            "bbraft_node_ingress_addr_valid": bbraft_node_ingress_addr_valid,
            "bbraft_node_guest_prefix_valid": bbraft_node_guest_prefix_valid,
            "bbraft_node_resolver_valid": bbraft_node_resolver_valid,
            "bbraft_node_gateway_valid": bbraft_node_gateway_valid,
            "bbraft_node_addr_in_prefix": bbraft_node_addr_in_prefix,
            "bbraft_node_addr_in_prefixes": bbraft_node_addr_in_prefixes,
            "bbraft_node_prefixes_overlap": bbraft_node_prefixes_overlap,
            "bbraft_node_ssh_port_range_valid": bbraft_node_ssh_port_range_valid,
            "bbraft_node_uint": bbraft_node_uint,
            "bbraft_node_ratio": bbraft_node_ratio,
            "bbraft_node_go_duration": bbraft_node_go_duration,
            "bbraft_node_byte_length": bbraft_node_byte_length,
            "bbraft_node_is_int": bbraft_node_is_int,
            "bbraft_node_mode_has": bbraft_node_mode_has,
            "bbraft_node_mode_lacks": bbraft_node_mode_lacks,
            "bbraft_node_systemd_exec_quote": bbraft_node_systemd_exec_quote,
            "bbraft_node_systemd_unit_quote": bbraft_node_systemd_unit_quote,
            "bbraft_node_path_ok": bbraft_node_path_ok,
            "bbraft_node_uid_range_valid": bbraft_node_uid_range_valid,
            "bbraft_node_block_device_valid": bbraft_node_block_device_valid,
            "bbraft_node_rate_valid": bbraft_node_rate_valid,
            "bbraft_node_rate_int": bbraft_node_rate_int,
            "bbraft_node_forward_guard": bbraft_node_forward_guard,
        }
