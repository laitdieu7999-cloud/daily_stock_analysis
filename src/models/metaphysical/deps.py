"""Optional dependency helpers for metaphysical research features."""

from __future__ import annotations


def get_ephem():
    try:
        import ephem  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "metaphysical astro features require optional dependency 'ephem'"
        ) from exc
    return ephem


def get_from_solar():
    try:
        from sxtwl import fromSolar  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "metaphysical ganzhi features require optional dependency 'sxtwl'"
        ) from exc
    return fromSolar


def dependencies_available() -> bool:
    try:
        get_ephem()
        get_from_solar()
    except RuntimeError:
        return False
    return True
