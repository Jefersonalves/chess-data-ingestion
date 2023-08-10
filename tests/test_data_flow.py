from unittest.mock import mock_open, patch

import pytest

from chess_data_ingestion.data_flow import (
    ChessDataIngestor,
    ChessMemoryDataSource,
    LocalDestination,
)


class TestChessMemoryDataSource:
    @pytest.mark.parametrize(
        "num_records, expected",
        [
            (-1, 0),
            (0, 0),
            (1, 1),
            (10, 10),
        ],
    )
    def test_load_len(self, num_records, expected):
        source = ChessMemoryDataSource()
        actual = source.load(num_records)
        assert len(actual) == expected

    def test_load_calls_chess_game_pgn(self):
        source = ChessMemoryDataSource()
        with patch.object(source.fake, "chess_game_pgn") as mock_chess_game_pgn:
            source.load(1)
            mock_chess_game_pgn.assert_called_once()


class TestLocalDestination:
    @patch("builtins.open", new_callable=mock_open)
    def test_save(self, mock_write):
        destination = LocalDestination(root_path="test", table_name="test")
        destination.save(data=[], file_format="pgn")
        mock_write.assert_called_once()


class TestChessDataIngestor:
    @patch.object(LocalDestination, "save")
    @patch.object(ChessMemoryDataSource, "load", return_value=[])
    def test_run(self, mock_load, mock_save):
        source = ChessMemoryDataSource()
        destination = LocalDestination(root_path="test", table_name="test")
        ingestor = ChessDataIngestor(source, destination)
        ingestor.run()
        mock_load.assert_called_once()
        mock_save.assert_called_once()
