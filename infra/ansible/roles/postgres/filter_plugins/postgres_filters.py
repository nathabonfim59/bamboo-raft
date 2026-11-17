"""Jinja filters for the postgres role.

Only the standard library is used, so the role-local unit test loads this file
without Ansible. Keep one strict integer parser so a float or a string such as
``"5432x"`` is rejected instead of being truncated by Jinja's ``| int``.
"""

from __future__ import annotations


def postgres_positive_int(value):
    """Return a positive decimal integer without truncating input.

    YAML integers and ASCII-decimal strings are accepted. Floats, bools,
    strings with a fraction/exponent/spaces and non-ASCII digits are rejected.
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


class FilterModule:
    """Expose the filters to Jinja."""

    def filters(self):
        return {"postgres_positive_int": postgres_positive_int}
