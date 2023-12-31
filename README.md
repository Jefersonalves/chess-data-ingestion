# chess-data-ingestion 👑
`chess-data-ingestion`  allows you to easily ingest fake chess data into your AWS S3 bucket or locally.

## Installation
Install the project by running:

```bash
git clone https://github.com/Jefersonalves/chess-data-ingestion
cd chess-data-ingestion
pip install .
```

## Usage

### CLI

To perform a local ingestion, run:

```bash
chess_data_ingestion -d local --root_path <path> --table_name <table>
```

If you want to ingest the data into your AWS S3 bucket, you need to setup your AWS credentials.
You can do this with the [AWS CLI](https://aws.amazon.com/cli/) by running:

```bash
aws configure
```

Now, you can perform the ingestion with the following command:

```bash
chess_data_ingestion -d s3 --bucket_name <bucket> --table_name <table>
```

This outputs a json file in the specified location.
The file is a malformed json to be compatible with AWS Athena, each line has a record representing a chess game.
The records has the following keys:

- `event`: The name of the event
- `site`: The name of the site
- `white`: The name of the white player
- `black`: The name of the black player
- `result`: The result of the game, e.g. 1-0 (white wins), 0-1 (black wins), 1/2-1/2 (draw)
- `utc_date`: The date of the game in UTC formated as YYYY.MM.DD
- `utc_time`: The time of the game in UTC formated as HH:MM:SS
- `white_elo`: The rating elo of the white player
- `black_elo`: The rating elo of the black player
- `white_rating_diff`: The rating elo difference of the white player
- `black_rating_diff`: The rating elo difference of the black player
- `eco`: The eco code of the opening
- `opening`: The name of the opening
- `time_control`: The time control of the game
- `termination`: The termination mode of the game, e.g. normal, time forfeit, abandoned
- `moves`: The moves of the game using the algebraic notation

The generated records are like:

```json
{
    "event": "Rated Blitz game",
    "site": "https://www.chess.com",
    "white": "hdias",
    "black": "slima",
    "result": "1-0",
    "utc_date": "2022.11.23",
    "utc_time": "06:54:54",
    "white_elo": 2629,
    "black_elo": 2532,
    "white_rating_diff": 89,
    "black_rating_diff": 38,
    "eco": "E60",
    "opening": "Kings Indian Defense",
    "time_control": "300+0",
    "termination": "Normal",
    "moves": "1. d4 Nf6 2. c4 g6 3. Nc3 Bg7"
}
```

### Module

If you need to create your own fake data, you can import and use the `chess_data_ingestion` module like this:

```python
from chess_data_ingestion.chess_provider import ChessProvider
from faker import Faker
fake = Faker()
fake.add_provider(ChessProvider)

# Generate a random chess opening like ('B20', 'Sicilian Defense', '1. e4 c5')
fake.chess_opening()

# Generate a random chess game
fake.chess_game()

# Generate a random chess game in pgn format
fake.chess_game_pgn()
```

## Development

This project uses [poetry](https://python-poetry.org/) to manage dependencies and [pre-commit](https://pre-commit.com/) to run some checks before each commit.
To contribute, install poetry and then install the project dependencies by running:

```bash
pip install poetry
poetry install
poetry run pre-commit install
```

To run the tests, execute:

```bash
poetry run pytest
```

## Extension

The project can be easily extended to support another chess data sources and destinations by creating your own DataIngestor
