from unittest.mock import patch

import pytest

from chess_data_ingestion.chess_provider import ChessProvider


class TestChessProvider:
    def test_chess_opening_type(self, fake_chess):
        actual = fake_chess.chess_opening()
        assert isinstance(actual, tuple)

    def test_chess_opening_len(self, fake_chess):
        actual = fake_chess.chess_opening()
        assert len(actual) == 3

    def test_chess_event_type(self, fake_chess):
        actual = fake_chess.chess_event()
        assert isinstance(actual, tuple)

    def test_chess_event_len(self, fake_chess):
        actual = fake_chess.chess_event()
        assert len(actual) == 2

    def test_chess_game_type(self, fake_chess):
        actual = fake_chess.chess_game()
        assert isinstance(actual, dict)

    @patch.object(ChessProvider, "chess_event")
    @patch.object(ChessProvider, "chess_opening")
    def test_chess_game_keys(self, mock_chess_opening, mock_chess_event, fake_chess):
        mock_chess_opening.return_value = ("B20", "Sicilian Defense", "1. e4 c5")
        mock_chess_event.return_value = ("Rated Bullet game", "60+0")
        actual = fake_chess.chess_game()
        expected_keys = {
            "event",
            "site",
            "white",
            "black",
            "result",
            "utc_date",
            "utc_time",
            "white_elo",
            "black_elo",
            "white_rating_diff",
            "black_rating_diff",
            "eco",
            "opening",
            "time_control",
            "termination",
            "moves",
        }

        assert set(actual.keys()) == expected_keys

    @pytest.mark.parametrize(
        "key, expected",
        [
            ("eco", "B20"),
            ("opening", "Sicilian Defense"),
            ("moves", "1. e4 c5"),
        ],
    )
    @patch.object(ChessProvider, "chess_opening")
    def test_chess_game_openings(self, mock_chess_opening, fake_chess, key, expected):
        mock_chess_opening.return_value = ("B20", "Sicilian Defense", "1. e4 c5")
        actual = fake_chess.chess_game()
        assert actual[key] == expected

    @pytest.mark.parametrize(
        "key, expected",
        [("event", "Rated Bullet game"), ("time_control", "60+0")],
    )
    @patch.object(ChessProvider, "chess_event")
    def test_chess_game_events(self, mock_chess_event, fake_chess, key, expected):
        mock_chess_event.return_value = ("Rated Bullet game", "60+0")
        actual = fake_chess.chess_game()
        assert actual[key] == expected

    @patch.object(ChessProvider, "chess_game")
    def test_chess_game_pgn(self, mock_chess_game, fake_chess):
        mock_chess_game.return_value = {
            "event": "Rated Bullet game",
            "site": "https://lichess.org",
            "white": "John",
            "black": "Doe",
            "result": "1-0",
            "utc_date": "2021.01.01",
            "utc_time": "12:00:00",
            "white_elo": 1500,
            "black_elo": 1600,
            "white_rating_diff": 10,
            "black_rating_diff": -5,
            "eco": "B20",
            "opening": "Sicilian Defense",
            "time_control": "120+1",
            "termination": "Normal",
            "moves": "1. e4 c5",
        }
        actual = fake_chess.chess_game_pgn()
        expected = """
            [Event "Rated Bullet game"]
            [Site "https://lichess.org"]
            [White "John"]
            [Black "Doe"]
            [Result "1-0"]
            [UTCDate "2021.01.01"]
            [UTCTime "12:00:00"]
            [WhiteElo "1500"]
            [BlackElo "1600"]
            [WhiteRatingDiff "10"]
            [BlackRatingDiff "-5"]
            [ECO "B20"]
            [Opening "Sicilian Defense"]
            [TimeControl "120+1"]
            [Termination "Normal"]

            1. e4 c5 1-0
        """
        assert actual.strip() == expected.strip()
