r"""Decode control characters from a VT100-sourced string.

Decodes following control codes:
'\x08' Backspace

These are all discarded currently:
'\x1b[K' DoEraseEndOfLine 
'\x1b[C' DoForwardOne
'\x1b[1P' "delete the character before the cursor and move all following \
characters back." -> Delete, but actually requires no action because readline \
 ses \x08 -> Discard
'\x1b[?-anything (only ?2004, ?1049 (results): consistently 4 numbers \
in psql output)

these are seemingly only in query results (discard all):
'\x1b[?1l' low cursor
'\x1b[?1h' high cursor 
'\x1b>' DoUpReverse
'\x1b[23;0;0'

default case for all remaining escapes: discard
"""
from typing import Callable, List, TextIO
from functools import reduce


class Vt100Decode:
    """Implements a simple state machine that applies \
    control codes like backspace found in a bytestring \
    and returns a plain text bytestring for humans and \
    parsers to read."""

    debug: bool = False

    def __init__(self, encoded_bytes: bytes):
        """Decodes the given bytestring, which can be retrieved via \
        Vt100Decode.get()."""
        self.enc: bytearray = bytearray(encoded_bytes)
        self.actions = []
        self.pos: int = 0

        # escape code actions:
        self._add_action(8, self._DoBackspace)  # \x08
        self._add_action(27, self._DoEraseCode)  # \x1b
        self._add_action(0, self._Stop)  # \0, means overflowing bytearray

        # loop decode until all bytes have been gone through
        while (self.pos < self._encLen() and self.pos >= 0):
            self._decode()

    def get(self):
        """Get decoded bytestring."""
        return bytes(self.enc)

    def _encLen(self):
        return reduce(lambda x, y: x + 1 if y > 0 else x, self.enc, 0)

    def _decode(self):
        default_case: bool = True

        for (a, f) in self.actions:
            if self.enc[self.pos] == a:
                f()
                default_case = False
                break

        # all actions have been gone through, move to next character
        if default_case:
            self.pos += 1

    def _add_action(self, code: int, action):
        self.actions.append((code, action))

    def _DoBackspace(self):
        """Delete backspace control code and previous character."""
        del self.enc[self.pos - 1:self.pos + 1]
        self.pos -= 1
        pass

    def _DoEraseCode(self):
        """Erase control code and associated parameters."""
        char_2: bytes = self.enc[self.pos + 1]

        if char_2 == 91:  # [
            # there are significant edge cases (esp. semicolon) where output \
            # is affected, but will do for initial testing:
            code_chars: bytes = b"0123456789?lhKCP;"
            length: int = 0
            isCode: bool = True

            while isCode:
                if self._encLen() > self.pos + 2 + length:
                    isCode = self.enc[self.pos + 2 + length] in code_chars
                else:
                    isCode = False
                if isCode:
                    length += 1
            del self.enc[self.pos:self.pos + 2 + length]
        elif char_2 == 62:  # >
            self.pos += 1  # TODO
        elif char_2 == 61:  # =
            self.pos += 1  # TODO
        else:
            raise ValueError("Unknown control code in psql output: "
                             + str(bytes(char_2)))

    def _Stop(self):
        self.pos = self._encLen()
