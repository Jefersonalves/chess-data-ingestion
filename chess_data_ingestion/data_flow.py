import datetime
import json
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

    def load(self, num_records: int, type: str = "json") -> List[str]:
        if type == "json":
            load_strategy = self._load_json
        elif type == "pgn":
            load_strategy = self._load_pgn
        else:
            raise ValueError(f"Type {type} not supported")

        return load_strategy(num_records=num_records)

    def _load_pgn(self, num_records: int) -> List[str]:
        return [self.fake.chess_game_pgn() for _ in range(num_records)]

    def _load_json(self, num_records: int) -> List[str]:
        return [self.fake.chess_game() for _ in range(num_records)]


class DataDestination(ABC):
    @abstractmethod
    def save(self, data: Union[List[str], List[dict]], **kwargs) -> None:
        pass


class LocalDestination(DataDestination):
    def __init__(self, root_path: str, table_name: str) -> None:
        self.root_path = root_path
        self.table_name = table_name

    def _create_table_partition(self, excution_datetime) -> None:
        extraction_date = excution_datetime.strftime("%Y-%m-%d")

        partition_path = Path(
            f"{self.root_path}/{self.table_name}/extracted_at={extraction_date}/"
        )
        partition_path.mkdir(parents=True, exist_ok=True)
        return partition_path

    def _create_file_name(self, partition_path, file_format, excution_datetime) -> str:
        extraction_time = excution_datetime.strftime("%H%M%S")
        file_name = f"{extraction_time}.{file_format}"
        file_path = partition_path / file_name
        return file_path

    def save(
        self,
        data: List[str],
        file_format,
        excution_datetime: datetime = datetime.datetime.now(),
    ) -> None:
        partition_path = self._create_table_partition(
            excution_datetime=excution_datetime
        )
        file_path = self._create_file_name(
            partition_path=partition_path,
            file_format=file_format,
            excution_datetime=excution_datetime,
        )

        if file_format == "json":
            write_strategy = self._write_json
        elif file_format == "pgn":
            write_strategy = self._write_pgn
        else:
            raise ValueError(f"File format {file_format} not supported")

        write_strategy(data=data, file_path=file_path)

    def _write_json(self, data: List[dict], file_path: str) -> None:
        with open(file_path, "w") as file:
            json.dump(data, file)

    def _write_pgn(self, data: List[str], file_path: str) -> None:
        with open(file_path, "w") as file:
            file.write("".join(data))


class S3Destination(DataDestination):
    def __init__(
        self, bucket_name: str, table_name: str, s3_client=boto3.client("s3")
    ) -> None:
        self.bucket_name = bucket_name
        self.table_name = table_name
        self.s3 = s3_client

    def save(
        self,
        data: List[str],
        file_format,
        excution_datetime: datetime = datetime.datetime.now(),
    ) -> None:
        extraction_date = excution_datetime.strftime("%Y-%m-%d")
        extraction_time = excution_datetime.strftime("%H%M%S")

        file_path = (
            f"{self.table_name}/extracted_at={extraction_date}/"
            f"{extraction_time}.{file_format}"
        )

        if file_format == "json":
            body = json.dumps(data)
        elif file_format == "pgn":
            body = "\n".join(data)
        else:
            raise ValueError(f"File format {file_format} not supported")

        self.s3.put_object(Bucket=self.bucket_name, Body=body, Key=file_path)


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
        data = self.source.load(num_records=num_records, type="json")

        self.destination.save(
            data=data, file_format="json", excution_datetime=excution_datetime
        )
