from typing import Dict, Tuple, Union

from faker.providers import BaseProvider


class ChessProvider(BaseProvider):
    """
    A Provider for generating fake Chess data

    Example
    -------
    >>> from faker import Faker
    >>> fake = Faker('pt_BR')
    >>> fake.add_provider(ChessProvider)
    >>> fake.chess_opening()
        ('B20', 'Sicilian Defense', '1. e4 c5')
    """

    openings = [
        ("C50", "Italian Game", "1. e4 e5 2. Nf3 Nc6 3. Bc4"),
        ("B20", "Sicilian Defense", "1. e4 c5"),
        ("C00", "French Defense", "1. e4 e6"),
        ("B10", "Caro-Kann Defense", "1. e4 c6"),
        ("C60", "Ruy Lopez (Spanish Opening)", "1. e4 e5 2. Nf3 Nc6 3. Bb5"),
        ("D10", "Slav Defense", "1. d4 d5 2. c4 c6"),
        ("D30", "Queens Gambit Declined", "1. d4 d5 2. c4 e6"),
        ("E60", "Kings Indian Defense", "1. d4 Nf6 2. c4 g6 3. Nc3 Bg7"),
        ("E20", "Nimzo-Indian Defense", "1. d4 Nf6 2. c4 e6 3. Nc3 Bb4"),
    ]

    events = [
        ("Rated Bullet game", "60+0"),
        ("Rated Bullet game", "120+1"),
        ("Rated Blitz game", "300+0"),
        ("Rated Blitz game", "300+3"),
        ("Rated Classical game", "1800+0"),
        ("Rated Classical game", "1800+5"),
    ]

    def chess_opening(self) -> Tuple[str, str, str]:
        """
        Randomly returns a Chess Opening  ('eco', 'name', 'moves').
        """
        return self.random_element(self.openings)

    def chess_event(self) -> Tuple[str, str]:
        """
        Randomly returns a Chess Event ('name', 'time_control').
        """
        return self.random_element(self.events)

    def chess_game(self) -> Dict[str, Union[str, int]]:
        """
        Randomly returns a Chess Game as a dictionary
        """
        opening = self.chess_opening()
        event = self.chess_event()
        return {
            "event": event[0],
            "site": self.random_element(
                ["https://lichess.org", "https://www.chess.com"]
            ),
            "white": self.generator.user_name(),
            "black": self.generator.user_name(),
            "result": self.random_element(["1-0", "0-1", "1/2-1/2"]),
            "utc_date": self.generator.date_between(
                start_date="-1y", end_date="today"
            ).strftime("%Y.%m.%d"),
            "utc_time": self.generator.time_object().strftime("%H:%M:%S"),
            "white_elo": self.random_int(min=1000, max=3000),
            "black_elo": self.random_int(min=1000, max=3000),
            "white_rating_diff": self.random_int(min=-100, max=100),
            "black_rating_diff": self.random_int(min=-100, max=100),
            "eco": opening[0],
            "opening": opening[1],
            "time_control": event[1],
            "termination": self.random_element(["Normal", "Time forfeit", "Abandoned"]),
            "moves": opening[2],
        }

    def chess_game_pgn(self) -> str:
        """
        Randomly returns a Chess Game in PGN format
        """
        game = self.chess_game()
        return f"""
            [Event "{game['event']}"]
            [Site "{game['site']}"]
            [White "{game['white']}"]
            [Black "{game['black']}"]
            [Result "{game['result']}"]
            [UTCDate "{game['utc_date']}"]
            [UTCTime "{game['utc_time']}"]
            [WhiteElo "{game['white_elo']}"]
            [BlackElo "{game['black_elo']}"]
            [WhiteRatingDiff "{game['white_rating_diff']}"]
            [BlackRatingDiff "{game['black_rating_diff']}"]
            [ECO "{game['eco']}"]
            [Opening "{game['opening']}"]
            [TimeControl "{game['time_control']}"]
            [Termination "{game['termination']}"]

            {game['moves']} {game['result']}
        """
