import pytest

from app.domain import IllegalStatusTransition, PaymentStatus, transition

PENDING = PaymentStatus.PENDING
SUCCEEDED = PaymentStatus.SUCCEEDED
FAILED = PaymentStatus.FAILED


@pytest.mark.parametrize("target", [SUCCEEDED, FAILED])
def test_pending_payment_can_be_finalized(target: PaymentStatus) -> None:
    assert transition(PENDING, target) is target


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (PENDING, PENDING),
        (SUCCEEDED, PENDING),
        (SUCCEEDED, SUCCEEDED),
        (SUCCEEDED, FAILED),
        (FAILED, PENDING),
        (FAILED, SUCCEEDED),
        (FAILED, FAILED),
    ],
)
def test_rejects_illegal_transition(current: PaymentStatus, target: PaymentStatus) -> None:
    with pytest.raises(IllegalStatusTransition) as error:
        transition(current, target)

    assert (error.value.current, error.value.target) == (current, target)


def test_only_pending_to_succeeded_and_pending_to_failed_are_allowed() -> None:
    allowed = set()
    for current in PaymentStatus:
        for target in PaymentStatus:
            try:
                transition(current, target)
            except IllegalStatusTransition:
                continue
            allowed.add((current, target))

    assert allowed == {(PENDING, SUCCEEDED), (PENDING, FAILED)}


def test_payment_statuses_are_exactly_pending_succeeded_failed() -> None:
    assert [status.value for status in PaymentStatus] == ["pending", "succeeded", "failed"]


def test_illegal_transition_error_message_names_both_statuses() -> None:
    with pytest.raises(IllegalStatusTransition) as error:
        transition(SUCCEEDED, FAILED)

    assert str(error.value) == "Illegal payment status transition: succeeded -> failed"
