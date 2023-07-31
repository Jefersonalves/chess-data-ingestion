from typing import Dict, Tuple, Union

from faker.providers import BaseProvider


class ChessProvider(BaseProvider):
    """
    A Provider for generating fake Chess data

    Example
    -------
    >>> from faker import Faker
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

    def chess_opening(self) -> Tuple[str, str, str]:
        """
        Randomly returns a Chess Opening  ('eco' , 'nome', 'main_line').
        """
        return self.random_element(self.openings)

    def chess_game(self) -> Dict[str, Union[str, int]]:
        """
        Randomly returns a Chess Game as a dictionary
        """
        return {
            "event": self.random_element(
                ["Rated Bullet game", "Rated Blitz game", "Rated Classical game"]
            ),
            "site": self.random_element(
                ["https://lichess.org", "https://www.chess.com"]
            ),
            "white": self.generator.user_name(),
            "black": self.generator.user_name(),
            "result": self.random_element(["1-0", "0-1", "1/2-1/2"]),
            "utcdate": self.generator.date_between(
                start_date="-2y", end_date="today"
            ).strftime("%Y.%m.%d"),
            "utctime": self.generator.time_object().strftime("%H:%M:%S"),
            "whiteelo": self.random_int(min=1000, max=3000),
            "blackelo": self.random_int(min=1000, max=3000),
            "whiteratingdiff": self.random_int(min=-100, max=100),
            "blackratingdiff": self.random_int(min=-100, max=100),
            "eco": self.random_element([x[0] for x in self.openings]),
            "opening": self.random_element([x[1] for x in self.openings]),
            "timecontrol": self.random_element(["60+0", "180+0", "300+0"]),
            "termination": self.random_element(["Normal", "Time forfeit", "Abandoned"]),
            "main_line": self.random_element([x[2] for x in self.openings]),
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
            [UTCDate "{game['utcdate']}"]
            [UTCTime "{game['utctime']}"]
            [WhiteElo "{game['whiteelo']}"]
            [BlackElo "{game['blackelo']}"]
            [WhiteRatingDiff "{game['whiteratingdiff']}"]
            [BlackRatingDiff "{game['blackratingdiff']}"]
            [ECO "{game['eco']}"]
            [Opening "{game['opening']}"]
            [TimeControl "{game['timecontrol']}"]
            [Termination "{game['termination']}"]

            {game['main_line']} {game['result']}
        """
