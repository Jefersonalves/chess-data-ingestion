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
    def __init__(self, root_path: str, table_name: str) -> None:
        self.root_path = root_path
        self.table_name = table_name

    def save(
        self,
        data: List[str],
        file_format,
        excution_datetime: datetime = datetime.datetime.now(),
    ) -> None:
        extraction_date = excution_datetime.strftime("%Y-%m-%d")
        extraction_time = excution_datetime.strftime("%H_%M_%S")

        partition_path = Path(
            f"{self.root_path}/{self.table_name}/" f"extracted_at={extraction_date}/"
        )
        partition_path.mkdir(parents=True, exist_ok=True)
        file_name = f"{extraction_time}.{file_format}"
        file_path = partition_path / file_name

        with open(file_path, "w") as file:
            file.write("".join(data))


class S3Destination(DataDestination):
    def __init__(self, bucket_name: str, table_name: str) -> None:
        self.bucket_name = bucket_name
        self.table_name = table_name
        self.s3 = boto3.client("s3")

    def save(
        self,
        data: List[str],
        file_format,
        excution_datetime: datetime = datetime.datetime.now(),
    ) -> None:
        extraction_date = excution_datetime.strftime("%Y-%m-%d")
        extraction_time = excution_datetime.strftime("%H_%M_%S")

        file_path = (
            f"{self.table_name}/extracted_at={extraction_date}/"
            f"{extraction_time}.{file_format}"
        )

        self.s3.put_object(Bucket=self.bucket_name, Body="".join(data), Key=file_path)


class DataIngestor(ABC):
    def __init__(self, source: DataSource, destination: DataDestination) -> None:
        self.source = source
        self.destination = destination

    @abstractmethod
    def run(self, **kwargs) -> None:
        pass


class ChessDataIngestor(DataIngestor):
    def run(self) -> None:
        excution_datetime = datetime.datetime.now()

        num_records = random.randint(100, 1000)
        data = self.source.load(num_records=num_records)

        self.destination.save(
            data=data, file_format="pgn", excution_datetime=excution_datetime
        )
