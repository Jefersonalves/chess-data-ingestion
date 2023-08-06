from chess_data_ingestion.data_flow import (
    ChessDataIngestor,
    ChessMemoryDataSource,
    LocalDestination,
)

if __name__ == "__main__":
    source = ChessMemoryDataSource()
    destination = LocalDestination()
    ingestor = ChessDataIngestor(source=source, destination=destination)

    ingestor.run(destination_root_path="data/chess")
