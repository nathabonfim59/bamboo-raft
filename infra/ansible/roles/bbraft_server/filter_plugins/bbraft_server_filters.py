"""Jinja filters for the bbraft_server role.

Only the standard library is used, so the filters also load in the role-local
unit test without Ansible. They keep systemd unit quoting and the listen-address
rules in one place:

* ``bbraft_systemd_exec_quote`` produces one ExecStart argument. It rejects
  line breaks (a newline cannot be represented inside one unit file line) and
  escapes ``%``, ``$``, ``"`` and ``\\`` according to systemd's unit syntax.
* ``bbraft_systemd_unit_quote`` does the same for settings such as
  ``ReadWritePaths=``, where ``$`` is literal and must not become ``$$``.
* ``bbraft_bind_is_public`` / ``bbraft_bind_is_loopback`` /
  ``bbraft_bind_is_wildcard`` mirror the Phase 0.5 rules in
  internal/server/config: a plaintext node listener is allowed only on
  loopback or private addresses, and the internal gateway must be loopback
  unless it has TLS.
* ``bbraft_positive_int`` and ``bbraft_nonnegative_int`` validate interval
  and percentage inputs without Jinja's silent truncation.
* ``bbraft_systemd_abs_path`` renders a verbatim absolute path for the two
  systemd settings that do not unquote their value (``EnvironmentFile=`` and
  ``LoadCredential=``).
* ``bbraft_public_url_valid``, ``bbraft_gateway_dial``,
  ``bbraft_dial_valid``, ``bbraft_dial_is_loopback`` and
  ``bbraft_gateway_server_name_valid`` mirror the accepted public URL,
  gateway dial and gateway trust rules in the same package.
"""

from __future__ import annotations

import ipaddress
import unicodedata
from urllib.parse import urlsplit

# Go's netip.IsPrivate is RFC 1918 plus fc00::/7; keep exactly those ranges so
# the role rejects and accepts the same addresses the binary does.
_PRIVATE_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("fc00::/7"),
)


def _split_host_port(addr, label="listen address"):
    """Return the host and port of a Go-style host:port address.

    Mirrors Go's net.SplitHostPort: IPv6 must be bracketed, so an unbracketed
    ``::1:7701`` is malformed rather than an IPv6 host with a port. The port
    must be ASCII decimal digits in 1-65535; Python's isdigit() also accepts
    Unicode digits, which Go's strconv.Atoi rejects.
    """
    text = str(addr)
    if text.startswith("["):
        end = text.find("]")
        if end < 0:
            raise ValueError(f"{label} {text!r}: missing closing ]")
        host = text[1:end]
        rest = text[end + 1 :]
        if not rest.startswith(":"):
            raise ValueError(f"{label} {text!r} is not [host]:port")
        port = rest[1:]
    else:
        host, sep, port = text.rpartition(":")
        if not sep:
            raise ValueError(f"{label} {text!r} is not host:port")
        if ":" in host:
            raise ValueError(
                f"{label} {text!r}: IPv6 addresses must be bracketed"
            )
    if not port or any(ch not in "0123456789" for ch in port):
        raise ValueError(f"{label} {text!r}: port must be ASCII digits")
    if not 1 <= int(port) <= 65535:
        raise ValueError(f"{label} {text!r} has a port outside 1-65535")
    return host, port


def _split_host(addr, label="listen address"):
    """Return only the host part of a Go-style host:port address."""
    return _split_host_port(addr, label)[0]


def _parse_ip(host, addr):
    try:
        ip = ipaddress.ip_address(host)
    except ValueError as err:
        raise ValueError(
            f"listen address {addr!r}: host must be localhost or an IP literal"
        ) from err
    # netip treats an IPv4-mapped IPv6 address as its IPv4 address for
    # IsLoopback/IsPrivate/IsLinkLocalUnicast. Python does not, so unmap.
    mapped = getattr(ip, "ipv4_mapped", None)
    return mapped if mapped is not None else ip


# Go's unicode.IsSpace: these seven code points plus the Unicode Zs, Zl and Zp
# categories (White_Space includes the line/paragraph separators).
_GO_SPACES = frozenset("\t\n\v\f\r \u0085\u00a0")
_GO_SPACE_CATEGORIES = frozenset(("Zs", "Zl", "Zp"))


def _go_is_space(ch):
    """Report whether Go's unicode.IsSpace accepts ch."""
    return ch in _GO_SPACES or unicodedata.category(ch) in _GO_SPACE_CATEGORIES


def _ascii_digits(text):
    """Report whether text is nonempty ASCII decimal digits only."""
    return bool(text) and all(ch in "0123456789" for ch in text)


_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


def _valid_percent_escapes(text):
    """Mirror net/url: every % must introduce two hexadecimal digits."""
    index = 0
    while True:
        index = text.find("%", index)
        if index < 0:
            return True
        if (
            index + 2 >= len(text)
            or text[index + 1] not in _HEX_DIGITS
            or text[index + 2] not in _HEX_DIGITS
        ):
            return False
        index += 3


def _valid_optional_port(rest):
    """Mirror Go's net/url validOptionalPort: empty or colon plus digits."""
    if rest == "":
        return True
    return rest.startswith(":") and (rest[1:] == "" or _ascii_digits(rest[1:]))


def bbraft_listen_valid(addr):
    """Return True for a host:port value the binary's config accepts."""
    host = _split_host(addr)
    if host and host != "localhost":
        _parse_ip(host, addr)
    return True


def bbraft_bind_is_public(addr):
    """Report whether a listen address exposes a socket beyond loopback/private.

    Matches internal/server/config.publicHost: an empty host is the wildcard
    and is public; localhost and private/loopback/link-local IPs are not.
    """
    host = _split_host(addr)
    if host == "localhost":
        return False
    if host == "":
        return True
    ip = _parse_ip(host, addr)
    if ip.is_loopback or ip.is_link_local:
        return False
    return not any(ip in network for network in _PRIVATE_NETWORKS)


def bbraft_bind_is_loopback(addr):
    """Report whether a listen address is loopback only."""
    host = _split_host(addr)
    if host == "localhost":
        return True
    if host == "":
        return False
    return _parse_ip(host, addr).is_loopback


def bbraft_bind_is_wildcard(addr):
    """Report whether a listen address binds every interface.

    Matches internal/server/config.wildcardListen: an empty host, 0.0.0.0 or
    :: has no stable public host, so the API role needs an explicit public URL.
    """
    return _split_host(addr) in ("", "0.0.0.0", "::")


def bbraft_public_url_valid(value):
    """Return True for an empty value or a trusted absolute http(s) base URL.

    Mirrors internal/server/config.publicURL: the scheme must be http or
    https, the hostname must be nonempty, and userinfo, a query and a fragment
    are rejected. A base path is allowed. Diagnostics name the setting only: a
    rejected URL may carry credentials or a secret query, so the raw value is
    never echoed.
    """
    text = str(value)
    if text == "":
        return True
    # Python's urlsplit strips a leading ASCII space; net/url.Parse rejects
    # that value, so do not let the stripped result look valid.
    if text.startswith(" "):
        raise ValueError("server.public-url: not a valid URL")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in text):
        raise ValueError("server.public-url: not a valid URL")
    if not _valid_percent_escapes(text):
        raise ValueError("server.public-url: not a valid URL")
    try:
        parts = urlsplit(text)
    except ValueError:
        # net/url rejects malformed authorities (for example an unterminated
        # IPv6 literal); the parser error would echo the URL, so detach it.
        raise ValueError("server.public-url: not a valid URL") from None
    if parts.scheme.lower() not in ("http", "https"):
        raise ValueError("server.public-url: scheme must be http or https")
    netloc = parts.netloc
    if "@" in netloc:
        raise ValueError(
            "server.public-url: URL must not carry credentials, a query or a fragment"
        )
    # Split the authority like Go's URL.Hostname: brackets for IPv6 and an
    # optional port that net/url only requires to be ASCII digits.
    if netloc.startswith("["):
        end = netloc.find("]")
        if end < 0 or not _valid_optional_port(netloc[end + 1 :]):
            raise ValueError("server.public-url: not a valid URL")
        host = netloc[1:end]
    else:
        host = netloc
        colon = netloc.rfind(":")
        if colon != -1:
            if not _valid_optional_port(netloc[colon:]):
                raise ValueError("server.public-url: not a valid URL")
            host = netloc[:colon]
    if host == "":
        raise ValueError("server.public-url: URL needs a host")
    if any(ch.isspace() or ch == "\0" for ch in host):
        raise ValueError("server.public-url: not a valid URL")
    if parts.query != "" or parts.fragment != "":
        raise ValueError(
            "server.public-url: URL must not carry credentials, a query or a fragment"
        )
    return True


def bbraft_gateway_dial(listen):
    """Return the dial target Go derives from a gateway listen address.

    Mirrors internal/server/config.dialHost: a wildcard bind (empty, 0.0.0.0 or
    ::) maps to 127.0.0.1, and IPv6 hosts are bracketed like net.JoinHostPort.
    """
    host, port = _split_host_port(listen)
    if host in ("", "0.0.0.0", "::"):
        host = "127.0.0.1"
    if ":" in host:
        return f"[{host}]:{port}"
    return f"{host}:{port}"


def bbraft_dial_valid(addr):
    """Return True for an empty value or a valid gateway dial target.

    Mirrors internal/server/config.dialAddr: host:port with a nonempty host
    (which may be a hostname) and an ASCII decimal port in 1-65535. Empty is
    valid because the binary then derives the target from the gateway listener.
    """
    text = str(addr)
    if text == "":
        return True
    host = _split_host(text, "node.gateway-dial")
    if host == "":
        raise ValueError(f"node.gateway-dial {text!r}: host must not be empty")
    return True


def bbraft_dial_is_loopback(addr):
    """Report whether a dial target stays on loopback (config.loopbackDial)."""
    try:
        host, _ = _split_host_port(addr, "node.gateway-dial")
    except ValueError:
        return False
    if host == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    mapped = getattr(ip, "ipv4_mapped", None)
    return (mapped if mapped is not None else ip).is_loopback


def bbraft_gateway_server_name_valid(value):
    """Return True for an empty value or a name without whitespace or NUL.

    Mirrors the node.gateway-server-name check in config.Validate, including
    Go's definition of whitespace; the rejected value is not echoed.
    """
    text = str(value)
    if text == "":
        return True
    if any(ch == "\0" or _go_is_space(ch) for ch in text):
        raise ValueError(
            "node.gateway-server-name must be nonempty without whitespace"
        )
    return True


def bbraft_systemd_exec_quote(value):
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


def bbraft_systemd_unit_quote(value):
    """Quote one value for a non-ExecStart systemd setting.

    Settings such as ``ReadWritePaths=`` do not perform environment-variable
    substitution, so a literal ``$`` must stay a single ``$``. Specifier
    expansion still applies, so ``%`` becomes ``%%``. Spaces stay inside the
    quoted value and a line break or NUL is rejected.
    """
    text = str(value)
    if "\n" in text or "\r" in text or "\0" in text:
        raise ValueError(f"value contains a line break or NUL: {text!r}")
    escaped = text.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    return f'"{escaped}"'


def bbraft_positive_int(value):
    """Return a positive decimal integer, without truncating input.

    YAML integers and ASCII-decimal strings are accepted. Floats, bools,
    strings with a fraction/exponent/spaces and non-ASCII digits are rejected
    instead of being truncated by Jinja's ``| int`` filter, so the rendered
    argv can never contain ``1.5`` or a silently changed value.
    """
    if isinstance(value, bool):
        raise ValueError(f"expected a positive integer, got boolean {value!r}")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value and all(
        ch in "0123456789" for ch in value
    ):
        number = int(value)
    else:
        raise ValueError(f"expected a positive decimal integer, got {value!r}")
    if number < 1:
        raise ValueError(f"expected a positive integer, got {number}")
    return number


def bbraft_systemd_abs_path(value):
    """Render one absolute path verbatim for EnvironmentFile= or LoadCredential=.

    systemd does not unquote those two settings, so a quoted value is not a
    path: it logs ``path is not absolute, ignoring`` or ``Credential source
    ... not valid, ignoring`` and starts the unit without the setting. This
    filter therefore returns the path unquoted and refuses anything that
    cannot be rendered safely: a relative path, whitespace, a quote, a
    backslash, a line break, NUL, a ``.`` or ``..`` element, or a trailing
    slash. Specifier expansion still applies, so every ``%`` becomes ``%%``.
    """
    text = str(value)
    if not text.startswith("/"):
        raise ValueError(f"path must be absolute: {text!r}")
    if text.endswith("/"):
        raise ValueError(f"path must not end with a slash: {text!r}")
    for ch in text:
        if ch == "\0":
            raise ValueError(f"path contains a NUL: {text!r}")
        if ch in "\"'\\":
            raise ValueError(f"path contains a quote or backslash: {text!r}")
        if _go_is_space(ch):
            raise ValueError(f"path contains whitespace: {text!r}")
    if any(part in (".", "..") for part in text.split("/")):
        raise ValueError(f"path contains a . or .. element: {text!r}")
    return text.replace("%", "%%")


def bbraft_nonnegative_int(value):
    """Return a nonnegative decimal integer, without truncating input.

    Used for settings where zero disables the feature (data retention, node
    loss, disk pressure). YAML integers and ASCII-decimal strings are
    accepted; floats, bools, strings with a fraction/exponent/spaces and
    non-ASCII digits are rejected.
    """
    if isinstance(value, bool):
        raise ValueError(f"expected a nonnegative integer, got boolean {value!r}")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value and all(
        ch in "0123456789" for ch in value
    ):
        number = int(value)
    else:
        raise ValueError(f"expected a nonnegative decimal integer, got {value!r}")
    if number < 0:
        raise ValueError(f"expected a nonnegative integer, got {number}")
    return number


def bbraft_effective_key_ids(keys):
    """Return the ids auth.LoadKeysFile will use.

    The loader defaults an empty id to the owner id, so the role must check
    uniqueness after that defaulting, not only the explicit ``id`` values.
    """
    return [key.get("id") or key.get("owner") for key in keys]


class FilterModule:
    """Expose the filters to Jinja."""

    def filters(self):
        return {
            "bbraft_systemd_exec_quote": bbraft_systemd_exec_quote,
            "bbraft_systemd_unit_quote": bbraft_systemd_unit_quote,
            "bbraft_bind_is_public": bbraft_bind_is_public,
            "bbraft_bind_is_loopback": bbraft_bind_is_loopback,
            "bbraft_bind_is_wildcard": bbraft_bind_is_wildcard,
            "bbraft_listen_valid": bbraft_listen_valid,
            "bbraft_public_url_valid": bbraft_public_url_valid,
            "bbraft_gateway_dial": bbraft_gateway_dial,
            "bbraft_dial_valid": bbraft_dial_valid,
            "bbraft_dial_is_loopback": bbraft_dial_is_loopback,
            "bbraft_gateway_server_name_valid": bbraft_gateway_server_name_valid,
            "bbraft_positive_int": bbraft_positive_int,
            "bbraft_nonnegative_int": bbraft_nonnegative_int,
            "bbraft_systemd_abs_path": bbraft_systemd_abs_path,
            "bbraft_effective_key_ids": bbraft_effective_key_ids,
        }
