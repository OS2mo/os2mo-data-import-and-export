# spsftp

An sftp-client for danish sftp.serviceplatformen.dk, using trigger- and metadata files as a means of routing things around to other users

This library has only taken the term called 'simple transfer' into account.  
This refers to paragraph 3.1 in this document [Vejledning til Serviceplatformens SFTP Service.pdf](https://share-komm.kombit.dk/P133/Ibrugtagning%20og%20test/Delte%20dokumenter/Vejledning%20til%20Serviceplatformens%20SFTP%20Service.pdf) as of June 13, 2019

A significant part of this library has been extracted from [cpr_udtraek](https://github.com/magenta-aps/cpr\_udtraek)

## Usage:

Create an instance of SpSftp and connect to the service

    from spsftp import SpSftp, MetadataError

    sp = SpSftp({
        "user": "int",
        "host": "sftp-287",
        "ssh_key_path": "/home/int/.ssh/id_rsa",
        "ssh_key_passphrase": "",
    })
    sp.connect()

SpSftp is just a thin wrapper around Paramikos [SFTPClient](http://docs.paramiko.org/en/latest/api/sftp.html), making use of it's putfo and getfo methods.
In order for You to use the rest of the SFTPClient, use SpSftps sftp object attribute.

    print(sp.sftp.listdir("OUT"))

Write a string 'hello-there' to a file named 'hellofile' in the OUT-folder on the server and ask for it to be transferred to the user 'kong-christian's IN-folder

    fl = io.BytesIO("hello-there".encode("utf-8"))
    sp.send(fl, "hellofile", "kong-christian")

If You have been writing into the file using its 'write' method, remember to reset filepointer before sending, if You want to send the whole file.

    fl.seek(0)

See what is currently in the incoming folder on the server

    print(sp.sftp.listdir("IN"))

Receive a file called 'hellofile' from user 'kong-kristian' and verify that it was actually sent from 'kong-kristian' and that I was indeed among the designated recipients. MetadataError will be raised if sender and receiver could not be verified.

    try:
        fl = io.BytesIO()
        sp.recv("hellofile", fl,  "kong-kristian")
    except MetadataError as e:
        print(e)  # e tells which validations that failed
        raise

Getting receipts for the sent files can be done using the sftp object attribute 

    fl = io.BytesIO()
    sp.sftp.getfo('hellofile.sftpreceipt', fl)

Disconnect from the service

    sp.disconnect()

