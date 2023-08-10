import datetime
import random
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Union

import boto3
from faker import Faker

from chess_data_ingestion.chess_provider import ChessProvider


class DataSource(ABC):
    @abstractmethod
    def load(self, **kwargs) -> List[str]:
        pass


class ChessMemoryDataSource(DataSource):
    def __init__(self) -> None:
        self.fake = Faker("pt_BR")
        self.fake.add_provider(ChessProvider)

    def load(self, num_records: int) -> List[str]:
        return [self.fake.chess_game_pgn() for _ in range(num_records)]


class DataDestination(ABC):
    @abstractmethod
    def save(self, data: Union[List[str], List[dict]], **kwargs) -> None:
        pass


class LocalDestination(DataDestination):
    def save(self, data: List[str], path: str) -> None:
        with open(f"{path}", "w") as file:
            file.write("".join(data))


class S3Destination(DataDestination):
    def __init__(self) -> None:
        self.s3 = boto3.client("s3")

    def save(self, data: List[str], bucket_name: str, path: str) -> None:
        self.s3.put_object(Bucket=bucket_name, Body="".join(data), Key=path)


class DataIngestor(ABC):
    def __init__(self, source: DataSource, destination: DataDestination) -> None:
        self.source = source
        self.destination = destination
        self.excution_datetime = datetime.datetime.now()

    @abstractmethod
    def run(self, **kwargs) -> None:
        pass


class S3ChessDataIngestor(DataIngestor):
    def run(self, bucket_name, destination_root_path: str) -> None:
        num_records = random.randint(100, 1000)
        data = self.source.load(num_records=num_records)

        extraction_date = self.excution_datetime.strftime("%Y-%m-%d")
        extraction_time = self.excution_datetime.strftime("%H:%M:%S")

        destination_file_path = (
            f"{destination_root_path}/extracted_at={extraction_date}/"
            f"{extraction_time}.pgn"
        )
        self.destination.save(
            data=data, bucket_name=bucket_name, path=destination_file_path
        )


class ChessDataIngestor(DataIngestor):
    def run(self, destination_root_path: str) -> None:
        num_records = random.randint(100, 1000)
        data = self.source.load(num_records=num_records)

        extraction_date = self.excution_datetime.strftime("%Y-%m-%d")
        extraction_time = self.excution_datetime.strftime("%H:%M:%S")

        partition_path = f"{destination_root_path}/extracted_at={extraction_date}"
        Path(partition_path).mkdir(parents=True, exist_ok=True)

        destination_file_path = f"{partition_path}/{extraction_time}.pgn"
        self.destination.save(data=data, path=destination_file_path)
