"""The typed-error table must cover the whole generated reason vocabulary."""

from __future__ import annotations

import pytest

import remount
from remount import errors
from remount.types import REASONS


def test_every_protocol_reason_has_a_class() -> None:
    assert REASONS, "the generated reason vocabulary is empty; the check proves nothing"
    assert errors.unmapped_reasons() == (), "add a class for each listed reason"
    table = errors.classes()
    assert set(table) == set(REASONS)
    for reason, cls in table.items():
        assert issubclass(cls, errors.ProtocolError)
        assert cls.REASON == reason


def test_factory_returns_the_typed_class() -> None:
    raised = errors.error_for("denied", "egress_denied", "api.example.com is not bound")
    assert isinstance(raised, errors.EgressDenied)
    assert isinstance(raised, errors.ProtocolError)
    assert raised.code == "denied"
    assert raised.reason == "egress_denied"
    assert raised.message == "api.example.com is not bound"
    assert str(raised) == "denied: api.example.com is not bound"

    with pytest.raises(errors.EgressDenied):
        errors.raise_for("denied", "egress_denied", "api.example.com is not bound")


def test_unknown_reason_falls_back_to_the_base_class() -> None:
    # A server newer than this package sends a reason it has never heard of.
    # That must degrade to ProtocolError, not raise from the table lookup.
    raised = errors.error_for("denied", "a_reason_from_the_future", "nope")
    assert type(raised) is errors.ProtocolError
    assert raised.reason == "a_reason_from_the_future"

    bare = errors.error_for("conflict", "", "workspace is pending")
    assert type(bare) is errors.ProtocolError
    assert bare.reason == ""


def test_evicted_carries_the_oldest_replayable_sequence() -> None:
    raised = errors.error_for("evicted", "output_evicted", "seq 3 is gone", 12)
    assert isinstance(raised, errors.OutputEvicted)
    assert raised.oldest == 12


def test_the_package_exposes_the_typed_errors() -> None:
    # The documented catch is `except remount.errors.EgressDenied`, so the
    # module has to be reachable from the package root.
    assert remount.errors.EgressDenied is errors.EgressDenied
    assert remount.ProtocolError is errors.ProtocolError
