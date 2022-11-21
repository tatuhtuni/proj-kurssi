from ..errfmt import ErrorFormatter

VT100_UNDERLINE = "\x1b[4m"
VT100_RESET = "\x1b[0m"


def test_format():
    warning = "Too many parentheses"
    warning_name = "SurplusParentheses"

    formatter = ErrorFormatter(warning, warning_name)
    warning_msg = formatter.format()
    assert warning_msg.find(warning) != -1 and warning_msg.find(warning_name) != -1

    warning = "Too many semicolons"
    warning_name = "SurplusSemicolons"
    underlined_query = f"""SELECT *
FROM customers
{VT100_UNDERLINE}WHERE type 'C'{VT100_RESET} = 1 OR 100 = 100;"""

    formatter = ErrorFormatter(warning, warning_name, underlined_query)
    warning_msg = formatter.format()
    assert (
        warning_msg.find(warning) != -1
        and warning_msg.find(warning_name) != -1
        and warning_msg.find(underlined_query) != -1
    )
