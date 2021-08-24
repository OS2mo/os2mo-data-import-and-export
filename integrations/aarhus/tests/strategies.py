from csv import DictWriter
from io import StringIO

import pydantic
from hypothesis import strategies as st


@st.composite
def csv_buf_from_model(draw, model: pydantic.BaseModel = None, delimiter: str = ";"):
    """Return a `StringIO` instance containing a single-item CSV built from
    the Pydantic model `model`.
    """
    instance = draw(st.builds(model))  # type: ignore
    buf = StringIO()
    csv_row = {
        getattr(field, "alias", key): getattr(instance, key)
        for key, field in instance.__fields__.items()
    }
    writer = DictWriter(buf, list(csv_row.keys()), delimiter=delimiter)
    writer.writeheader()
    writer.writerow(csv_row)
    return buf
