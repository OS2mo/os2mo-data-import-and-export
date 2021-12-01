import tempfile
import unittest

from hypothesis import given
from hypothesis.strategies import text

from integrations.opus.opus_file_reader import get_opus_filereader
from integrations.opus.opus_file_reader import LocalOpusReader


class OpusFileReader_test(unittest.TestCase):
    """
    Tests for opus file reader interface
    """

    def test_local(self):
        settings = {}
        ofr = get_opus_filereader(settings=settings)
        assert isinstance(ofr, LocalOpusReader)

    def test_empty_path(self):
        with tempfile.TemporaryDirectory() as tmppath:
            settings = {"integrations.opus.import.xml_path": tmppath}

            ofr = get_opus_filereader(settings=settings)
            assert len(ofr.list_opus_files()) == 0

    @given(text())
    def test_text_in_file(self, dummy_text):
        dummy_text = repr(dummy_text)
        filename = "/ZLPB20100101145823.xml"
        with tempfile.TemporaryDirectory() as tmppath:
            with open(tmppath + filename, "w") as opus_file:
                opus_file.write(dummy_text)

            settings = {"integrations.opus.import.xml_path": tmppath}
            ofr = get_opus_filereader(settings=settings)
            files_list = ofr.list_opus_files()
            assert len(files_list) == 1
            text_in = ofr.read_latest()
            assert text_in == dummy_text


if __name__ == "__main__":
    unittest.main()
