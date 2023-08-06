import pytest
from faker import Faker

from chess_data_ingestion.chess_provider import ChessProvider


@pytest.fixture
def fake_chess():
    fake = Faker()
    fake.add_provider(ChessProvider)
    return fake
