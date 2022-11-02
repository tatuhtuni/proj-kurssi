r"""Decode control characters from a VT100-sourced string.

Inspired by https://github.com/noahspurrier/pexpect/blob/master/ANSI.py

TODO:
detect ^C
extend psqlwrapper to always include text from all the way from start of the line (meaning "db=> ") so that \r + cursor moves can be calculated right

supporting ctrl-R seems to have a ton of edge cases, and not even pexpect handles it gracefully.

Executes following control codes:
'\x08' Backspace
'\r' MoveToStartOfLine
'\x1b[C' DoForwardOne
'\x1b[K' DoEraseEndOfLine

Leaves as is for heuristics:
'\n' newline

These are all discarded currently:
'\x1b[1P' "delete the character before the cursor and move all following \
characters back." -> Delete, but actually requires no action because readline \
 adds \x08? -> Discard
'\x1b[?-anything (only ?2004, ?1049 (results): consistently 4 numbers \
in psql output)
'\x1b[A DoUp psql prompt technically allows for multiline editing, but this is not supported currently

these are seemingly only in query results (discard all):
'\x1b[?1l' low cursor
'\x1b[?1h' high cursor 
'\x1b>' DoUpReverse
'\x1b[23;0;0'

default case for all remaining escapes: discard
"""
from functools import partial


class Vt100Decode:
    """Implements a simple state machine that \
    by implementing a subset of vt100 applies \
    control codes like backspace found in a bytestring \
    and returns a plain text bytestring for humans and \
    parsers to read."""

    debug: bool = False

    def __init__(self, encoded_bytes: bytes):
        """Decode the given bytestring, which can be retrieved via \
        Vt100Decode.get()."""
        self.enc: bytearray = bytearray(encoded_bytes)
        
        self.actions = []  # type hint: List[int, Callable[something]]
        self.stack = []  # type hint: List[Callable[something]]
        # encoded_bytes gives upper limit to bytestring length:
        self.array_size: int = len(encoded_bytes)  # save this for later use
        self.dec: bytearray = bytearray(self.array_size)

        # keep track of positions within bytearrays
        self.enc_pos: int = 0
        self.stack_pos: int = 0
        self.dec_pos: int = 0

        # simple escape code actions:
        self._add_action(8, self._DoBackspace)  # \x08
#        self._add_action(27, self._DoEraseCode)  # \x1b
#        self._add_action(0, self._Stop)  # \0, means overflowing bytearray;
#        except with current implementation \0 can be anywhere
        # _decode() has a default case (for text) that runs self._DoWrite
        
        # fill stack until all bytes have been gone through
        while (self.enc_pos < self.array_size and self.enc_pos >= 0):
            self._decode()

        # execute generated action stack
        self._executeStack()

    def get(self):
        """Get decoded bytestring."""
        x = bytes(self.dec).replace(b'\x00', b'')
        print("enc:" + str(bytes(self.enc)) + "\r\n")
        print("dec:" + str(x) + "\r\n")
        print("dec_str: " + bytes.decode(x) + "\r\n")
        return x

    def _decode(self):
        default_case: bool = True

        for (a, f) in self.actions:
            if self.enc[self.enc_pos] == a:
                self.stack.append(f)
                default_case = False
                self.enc_pos += 1
                break

        # checking default_case in every if-clause to avoid reads
        # from multiple enc_pos

        # in our case \r means different things if \n directly follows
        if default_case and self.enc[self.enc_pos] == 13:  # \r
            # no \n possible if at end of bytestring
            # but also means we can disregard \r
            self.enc_pos += 1  # we can move to next unread character
            if self.array_size == self.enc_pos:
                pass
            elif self.enc[self.enc_pos] == 10:  # \n
                pass
            else:
                self.stack.append(self._DoCarriageReturn)
            default_case = False

        # handle select cases of \x1b in an ad-hoc way, as vast majority of
        # cases are simply discarded in decoding simple bytestrings:
        if default_case and self.enc[self.enc_pos] == 27:
            self._handle_x1b()
            default_case = False

        # all actions have been gone through, move to next character
        if default_case:
            self.stack.append(partial(self._DoWrite, self.enc[self.enc_pos]))
            self.enc_pos += 1

    def _add_action(self, code: int, action):
        self.actions.append((code, action))

    def _DoBackspace(self):
        """Delete the character before current position in decoded string \
        and move back."""
        self.dec_pos -= 1
        if self.dec[self.dec_pos] == 0:  # account for 0-cells
            self._DoBackspace()
        else:
            del self.dec[self.dec_pos]

    def _DoCarriageReturn(self):
        """Move position to start of line."""
        # move until before \n or start of string
        while self.dec_pos > 0 \
              and self.dec[self.dec_pos - 1] != 10:
            self.dec_pos -= 1

    def _DoEraseEndOfLine(self):
        while self.dec_pos < len(self.dec) \
              and self.dec[self.dec_pos] != 10:  # \n
            del self.dec[self.dec_pos]

    def _DoForwardOne(self):
        self.dec_pos += 1
        # 0-cells are an anomaly produced by bytearray, skip them:
        if self.dec[self.dec_pos] == 0:
            self._DoForwardOne()

    def _handle_x1b(self):
        """Discard control code and associated parameters."""
        char_2: bytes = self.enc[self.enc_pos + 1]

        if char_2 == 91:  # [
            char_3: bytes = self.enc[self.enc_pos + 2]
            char_3_discards: bytes = b"HDBAJrm"  # no parameter discards
            char_3_param: bytes = b"0123456789"
            
            self.enc_pos += 3  # this is the next unread character, "char_4"

            if char_3 in char_3_discards:
                pass
            elif char_3 == 63:  # '?'
                # ? with a string of digits that always ends with h/l
                # discard everything
                end_param: bytes = b"hl"
                param_length: int = 0

                while self.enc[self.enc_pos + param_length] not in end_param:
                    param_length += 1

                self.enc_pos += param_length + 1
            elif char_3 == 67:  # C = DoForwardOne
                self.stack.append(self._DoForwardOne)
            elif char_3 == 75:  # K = EraseEndOfLine
                self.stack.append(self._DoEraseEndOfLine)
            elif char_3 in char_3_param:
                # up to infinite number prefix parameters with
                # ; delimiter ending in m/t. However, only
                # up to 2 parameters are ever seen in wild.
                # discard everything
                params: bytes = b"0123456789;"
                params_end: bytes = b"mtJP@"
                params_length: int = 0

                while self.enc[self.enc_pos + params_length] in params:
                    params_length += 1
                if self.enc[self.enc_pos + params_length] in params_end:
                    self.enc_pos += params_length + 1
                else:
                    raise ValueError("num[;num] parameter ended in unexpected \
                    code character: " + str(self.enc[
                        self.enc_pos - 3:self.enc_pos + params_length + 1]))
            else:
                raise ValueError("Unknown \\x1b[-code. Is this non-VT100 terminal?")
        elif char_2 == 62:  # >
            self.enc_pos += 2  # TODO
        elif char_2 == 61:  # =
            self.enc_pos += 2  # TODO
        else:
            raise ValueError("Unknown control code in psql output: "
                             + str(bytes(char_2)))

    def _DoWrite(self, char: bytes):
        self.dec[self.dec_pos:self.dec_pos] = bytearray(chr(char), "utf-8")
        self.dec_pos += 1

    def _executeStack(self):
        for f in self.stack:
            f()

    def _Stop(self):
        self.enc_pos = -1
