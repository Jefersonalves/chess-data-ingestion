from unittest.mock import mock_open, patch

import botocore.session
import pytest
from botocore.stub import ANY, Stubber

from chess_data_ingestion.data_flow import (
    ChessDataIngestor,
    ChessMemoryDataSource,
    LocalDestination,
    S3Destination,
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
            source.load(num_records=1, type="pgn")
            mock_chess_game_pgn.assert_called_once()


class TestLocalDestination:
    @patch("builtins.open", new_callable=mock_open)
    def test_save(self, mock_write):
        destination = LocalDestination(root_path="test", table_name="test")
        destination.save(data=[], file_format="pgn")
        mock_write.assert_called_once()


class TestS3Destination:
    def test_save(self):
        session = botocore.session.get_session()
        s3 = session.create_client("s3")
        stubber = Stubber(s3)
        stubber.add_response(
            "put_object",
            {},
            {
                "Bucket": "test",
                "Key": ANY,
                "Body": "",
            },
        )

        destination = S3Destination(bucket_name="test", table_name="test", s3_client=s3)
        stubber.activate()
        destination.save(data=[], file_format="pgn")
        stubber.assert_no_pending_responses()


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
