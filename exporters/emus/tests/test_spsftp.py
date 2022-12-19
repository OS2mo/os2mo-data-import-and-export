import unittest
from spsftp.spsftp import SpSftp, MetadataError
import io

xmlfile = """<ns2:Trigger
xmlns:ns2="http://serviceplatformen.dk/xml/wsdl/soap11/SFTP/1/types">
<FileTransferUUID>
47570fc2-be65-4ab1-8da7-3bcc086bfef9
</FileTransferUUID>
<FileDescriptor>
<FileName>hellofile</FileName>
<SizeInBytes>11</SizeInBytes>
<Sender>kong-christian</Sender>
<Recipients>x</Recipients>
</FileDescriptor>
</ns2:Trigger>
"""
hellofile = "hello there"


class SftpMock:

    st_size = 42

    def getfo(self, remotepath, fl):
        if remotepath == "IN/hellofile.metadata":
            fl.write(xmlfile.encode("utf-8"))
        else:
            fl.write(hellofile.encode("utf-8"))
        return self

    def putfo(self, fl, remotepath):
        if remotepath == "OUT/hellofile.trigger":
            self.triggerfile = fl
        return self


class SpSftpMock(SpSftp):

    sftp_client = SftpMock()

    def get_key(self, filename, password):
        return "Key"

    def get_transport(self):
        return "Transport"

    def connect(self):
        pass

    def disconnect(self):
        pass


class Tests(unittest.TestCase):
    def setUp(self):
        self.spsftp = SpSftpMock(
            {
                "user": "x",
                "host": "y",
                "ssh_key_path": "",
                "ssh_key_passphrase": "",
            }
        )

    def test_creates_key_and_transport(self):
        self.assertEqual(self.spsftp.key, "Key")
        self.assertEqual(self.spsftp.transport, "Transport")

    def test_recv_unknown_sender(self):
        with self.assertLogs("spsftp", level="WARNING"):
            fl = io.BytesIO()
            with self.assertRaisesRegex(
                MetadataError, "Sender kong-christian not acknowledged as kong-kristian"
            ):
                self.spsftp.recv("hellofile", fl, "kong-kristian")

    def test_recv_wrong_recipient(self):
        with self.assertLogs("spsftp", level="WARNING"):
            fl = io.BytesIO()
            self.spsftp.username = "A"
            with self.assertRaisesRegex(MetadataError, "A not in Recipients: x"):
                self.spsftp.recv("hellofile", fl, "kong-christian")

    def test_recv_unknown_sender_and_wrong_recipient(self):
        with self.assertLogs("spsftp", level="WARNING"):
            fl = io.BytesIO()
            self.spsftp.username = "A"
            with self.assertRaisesRegex(
                MetadataError,
                "Sender kong-christian not acknowledged as kong-kristian"
                ", A not in Recipients: x",
            ):
                self.spsftp.recv("hellofile", fl, "kong-kristian")

    def test_recv_good(self):
        with self.assertLogs("spsftp", level="INFO"):
            fl = io.BytesIO()
            self.spsftp.recv("hellofile", fl, "kong-christian")
            self.assertEqual(hellofile, fl.getvalue().decode("utf-8"))

    def test_send(self):
        with self.assertLogs("spsftp", level="INFO"):
            fl = io.BytesIO("hello-there".encode("utf-8"))
            self.spsftp.send(fl, "hellofile", "kong-christian")
