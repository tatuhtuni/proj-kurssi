from typing import Optional


class ErrorFormatter:
    def __init__(
        self,
        warning_msg: str,
        warning_name: str,
        underlined_query: Optional[str] = None,
    ):
        self.warning_name: str = warning_name
        self.warning_msg: str = warning_msg
        self.underlined_query: Optional[str] = underlined_query

    def format(self) -> str:
        """
        Returns a formatted error message.
        """

        base_msg = f"Warning: {self.warning_msg} [pg4n::{self.warning_name}]"
        if self.underlined_query:
            return base_msg + f"\n{self.underlined_query}"
        return base_msg
