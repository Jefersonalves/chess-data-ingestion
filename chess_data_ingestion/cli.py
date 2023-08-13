import argparse

from chess_data_ingestion.data_flow import (
    ChessDataIngestor,
    ChessMemoryDataSource,
    LocalDestination,
    S3Destination,
)


def run_command_line():
    parser = argparse.ArgumentParser(description="Chess Data Ingestion CLI")
    parser.add_argument(
        "-d",
        "--destination",
        choices=["local", "s3"],
        required=True,
        help='Defines the destination for the data: "local" or "s3"',
    )
    parser.add_argument("--bucket_name", help="S3 bucket name.")
    parser.add_argument("--root_path", help="Local root path.")
    parser.add_argument("--table_name", required=True, help="Table name.")
    args = parser.parse_args()

    source = ChessMemoryDataSource()

    if args.destination == "s3":
        if args.bucket_name is None:
            parser.error("--bucket_name is required when destination is 's3'.")

        destination = S3Destination(
            bucket_name=args.bucket_name, table_name=args.table_name
        )
    else:
        if args.root_path is None:
            parser.error("--root_path is required when destination is 'local'.")

        destination = LocalDestination(
            root_path=args.root_path, table_name=args.table_name
        )

    ingestor = ChessDataIngestor(source=source, destination=destination)
    ingestor.run()
