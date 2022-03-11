from csv import DictWriter
from io import StringIO

import pydantic
from hypothesis import strategies as st


def text_except(*exceptions: str):
    """Return an `st.text` strategy where certain characters are not present."""
    return st.text(
        alphabet=st.characters(
            # Exclude our list of excepted characters
            blacklist_characters=exceptions,
            # Exclude surrogate characters because they are invalid in the
            # UTF-8 encoding. See:
            # https://hypothesis.readthedocs.io/en/latest/data.html#hypothesis.strategies.text
            blacklist_categories=("Cs",),
        )
    )


@st.composite
def csv_buf_from_model(draw, model: pydantic.BaseModel, delimiter: str = ";"):
    """Return a `StringIO` instance containing a single-item CSV built from
    the Pydantic model `model`.
    """
    # The `csv` module does not handle ASCII NUL characters well. Since we are
    # not testing the `csv` module itself, but rather the code utilizing it,
    # we'll ensure we do not produce sample CSV data containing ASCII NULs.
    # We'll also make sure to not include the CSV delimiter character in the
    # values produced for the CSV fields, as this will also cause problems when
    # constructing a mock CSV buffer.
    ascii_nul = chr(0)
    overrides = {
        fieldname: text_except(ascii_nul, delimiter)
        for fieldname, field in model.__fields__.items()
        if issubclass(field.type_, str)
    }

    # Construct a strategy based on the Pydantic model `model` and the contents
    # of `overrides`.
    strategy: st.SearchStrategy = st.builds(model, **overrides)  # type: ignore

    # Construct a buffer containing a one-item CSV file based on our `strategy`
    instance = draw(strategy)  # type: ignore
    buf = StringIO()
    csv_row = {
        getattr(field, "alias", key): getattr(instance, key)
        for key, field in instance.__fields__.items()
    }
    writer = DictWriter(buf, list(csv_row.keys()), delimiter=delimiter, escapechar="\\")
    writer.writeheader()
    writer.writerow(csv_row)
    return buf
