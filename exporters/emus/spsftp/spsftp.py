#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import paramiko
import logging
import xmltodict
import hashlib
import uuid
import io

logger = logging.getLogger("spsftp")

triggerfile = """<ns2:Trigger
xmlns:ns2="http://serviceplatformen.dk/xml/wsdl/soap11/SFTP/1/types">
<FileDescriptor>
<FileName>%(filename)s</FileName>
<SizeInBytes>%(filesize)s</SizeInBytes>
<Sender>%(sender)s</Sender>
<SendersFileId>%(fileid)s</SendersFileId>
<Recipients>%(recipient)s</Recipients>
</FileDescriptor>
<FileContentDescriptor>
</FileContentDescriptor>
</ns2:Trigger>"""


class MetadataError(Exception):
    pass


class SpSftp(object):
    """Upload and fetch from serviceplatformen
    providing trigger and using metadata-files
    """

    def __init__(self, settings={}):
        """Constructor.
        :param settings: user, host, port, ssh_key_path, ssh_key_passphrase
        :type settings: dict
        :return: void
        :rtype: None"""

        self.username = settings.get("user")
        self.host = settings.get("host", "sftp.serviceplatformen.dk")
        self.port = int(settings.get("port", 22))

        self.key = self.get_key(
            filename=settings.get("ssh_key_path"),
            password=settings.get("ssh_key_passphrase"),
        )

        self.transport = self.get_transport()

    def get_key(self, filename, password):
        return paramiko.RSAKey.from_private_key_file(filename, password)

    def get_transport(self):
        return paramiko.Transport((self.host, self.port))

    def connect(self):
        """Opens connection to sftp server.
        :return: void
        :rtype: None"""

        self.transport.connect(username=self.username, pkey=self.key)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def disconnect(self):
        """Closes connection to sftp server.
        :return: void
        :rtype: None"""

        self.sftp.close()
        self.transport.close()
        self.sftp = None

    def send(self, fl, filename, recipient):
        """Upload both file and triggerfile
        to serviceplatformen OUT folder
        """
        remotepath = "OUT/" + filename
        sender = self.username
        fileid = str(uuid.uuid4())
        logger.debug("uploading %s", remotepath)
        filesize = self.sftp.putfo(fl, remotepath).st_size
        logger.debug("uploading %s.trigger", remotepath)
        self.sftp.putfo(
            io.StringIO(
                triggerfile
                % {
                    "filename": filename,
                    "sender": sender,
                    "fileid": fileid,
                    "recipient": recipient,
                    "filesize": filesize,
                }
            ),
            remotepath + ".trigger",
        )
        logger.info("sent: %s (SendersFileId: %s) to %s", filename, fileid, recipient)

    def recv(self, filename, fl, sender):
        """Download both file and metadatafile
        from serviceplatformen IN folder
        """
        remotepath = "IN/" + filename
        metafl = io.BytesIO()
        logger.debug("downloading %s.metadata", remotepath)
        self.sftp.getfo(
            remotepath + ".metadata",
            metafl,
        )

        metadata = xmltodict.parse(metafl.getvalue())["ns2:Trigger"]
        xferid = metadata["FileTransferUUID"]
        filedescriptor = metadata["FileDescriptor"]

        errors = []
        if sender != filedescriptor["Sender"]:
            errors.append(
                "Sender %s not acknowledged as %s" % (filedescriptor["Sender"], sender)
            )
        if self.username not in filedescriptor["Recipients"]:
            errors.append(
                "%s not in Recipients: %s"
                % (self.username, filedescriptor["Recipients"])
            )

        if errors:
            logger.warning(
                "ignoring '%s' (FileTransferUUID: %s)"
                " because of errors in %s: %r "
                % (filename, xferid, filename + ".metadata", errors)
            )
            raise MetadataError(
                "File %s (FileTransferUUID: %s): " % (filename, xferid)
                + ", ".join(errors)
            )
        else:
            self.sftp.getfo(remotepath, fl)
            logger.info(
                "succesfully fetched and validated %s from %s",
                filename,
                filedescriptor["Sender"],
            )
