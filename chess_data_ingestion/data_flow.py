from abc import ABC, abstractmethod
from typing import List, Union

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


class DataIngestor(ABC):
    def __init__(self, source: DataSource, destination: DataDestination) -> None:
        self.source = source
        self.destination = destination

    @abstractmethod
    def run(self, **kwargs) -> None:
        pass


class ChessDataIngestor(DataIngestor):
    def run(self, source_num_records: int, destination_path: str) -> None:
        data = self.source.load(num_records=source_num_records)
        self.destination.save(data=data, path=destination_path)
