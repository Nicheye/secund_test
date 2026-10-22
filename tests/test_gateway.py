import random
from decimal import Decimal

import pytest

from app.config import Settings
from app.domain import PaymentStatus
from app.gateway import EmulatedGateway
from app.models import Payment


class ScriptedRandom(random.Random):
    """random() returns the scripted values in order; uniform() consumes one of them."""

    def __init__(self, values: list[float]) -> None:
        super().__init__()
        self._values = list(values)

    def random(self) -> float:
        return self._values.pop(0)


class SleepRecorder:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def make_settings(**overrides: float) -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://unused",
        rabbitmq_url="amqp://unused",
        api_key="unused",
        _env_file=None,
        **overrides,  # type: ignore[arg-type]
    )


def make_payment() -> Payment:
    return Payment(amount=Decimal("1.00"), currency="USD")


@pytest.mark.parametrize(
    ("success_rate", "roll", "expected"),
    [
        (0.9, 0.0, PaymentStatus.SUCCEEDED),
        (0.9, 0.8999999, PaymentStatus.SUCCEEDED),
        (0.9, 0.9, PaymentStatus.FAILED),
        (0.9, 0.9000001, PaymentStatus.FAILED),
        (0.9, 0.9999999, PaymentStatus.FAILED),
        (0.0, 0.0, PaymentStatus.FAILED),
        (1.0, 0.9999999, PaymentStatus.SUCCEEDED),
        (0.5, 0.4999, PaymentStatus.SUCCEEDED),
        (0.5, 0.5, PaymentStatus.FAILED),
    ],
)
async def test_gateway_outcome_flips_exactly_at_the_success_rate(
    success_rate: float, roll: float, expected: PaymentStatus
) -> None:
    gateway = EmulatedGateway(
        make_settings(gateway_success_rate=success_rate),
        sleeper=SleepRecorder(),
        rng=ScriptedRandom([0.0, roll]),
    )

    assert await gateway.process(make_payment()) == expected


@pytest.mark.parametrize(
    ("uniform_roll", "expected_delay"), [(0.0, 2.0), (0.5, 3.5), (0.999999, 4.999997), (1.0, 5.0)]
)
async def test_gateway_sleeps_once_for_a_delay_between_min_and_max_seconds(
    uniform_roll: float, expected_delay: float
) -> None:
    sleeper = SleepRecorder()
    gateway = EmulatedGateway(
        make_settings(), sleeper=sleeper, rng=ScriptedRandom([uniform_roll, 0.0])
    )

    await gateway.process(make_payment())

    assert sleeper.delays == [pytest.approx(expected_delay)]


async def test_gateway_uses_configured_delay_range() -> None:
    sleeper = SleepRecorder()
    gateway = EmulatedGateway(
        make_settings(gateway_min_seconds=10, gateway_max_seconds=20),
        sleeper=sleeper,
        rng=ScriptedRandom([0.5, 0.0]),
    )

    await gateway.process(make_payment())

    assert sleeper.delays == [15.0]


async def test_gateway_with_a_real_random_source_always_sleeps_within_bounds() -> None:
    sleeper = SleepRecorder()
    gateway = EmulatedGateway(make_settings(), sleeper=sleeper, rng=random.Random(7))

    for _ in range(200):
        await gateway.process(make_payment())

    assert len(sleeper.delays) == 200
    assert all(2.0 <= delay <= 5.0 for delay in sleeper.delays)


async def test_gateway_without_injected_dependencies_uses_asyncio_sleep_and_a_random_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)

    monkeypatch.setattr("app.gateway.asyncio.sleep", fake_sleep)
    gateway = EmulatedGateway(make_settings(gateway_min_seconds=0, gateway_max_seconds=0))

    outcome = await gateway.process(make_payment())

    assert outcome in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}
