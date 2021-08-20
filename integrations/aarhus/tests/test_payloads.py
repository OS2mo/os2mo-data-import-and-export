from hypothesis import given
from hypothesis import strategies as st

import payloads


class TestConvertCreateToEdit:
    @given(st.dictionaries(keys=st.text(), values=st.text()), st.text(), st.text())
    def test_convert_no_from_date(self, payload, uuid_arg, type_arg):

        payload["uuid"] = uuid_arg
        payload["type"] = type_arg

        actual = payloads.convert_create_to_edit(payload)

        assert actual["data"] == payload
        assert actual["uuid"] == uuid_arg
        assert actual["type"] == type_arg

    @given(
        st.dictionaries(keys=st.text(), values=st.text()),
        st.text(),
        st.text(),
        st.text(min_size=1),
    )
    def test_convert_with_from_date(self, payload, uuid_arg, type_arg, from_date):

        payload["uuid"] = uuid_arg
        payload["type"] = type_arg

        actual = payloads.convert_create_to_edit(payload, from_date)

        assert actual["data"]["validity"]["from"] == from_date
        assert actual["uuid"] == uuid_arg
        assert actual["type"] == type_arg
