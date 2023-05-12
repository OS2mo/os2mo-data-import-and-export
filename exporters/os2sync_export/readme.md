# OS2sync_export

## Equivalence tests

By configuring the settings `os2sync_url` to "stub" the exporter wil use a dummy client instead of writing to os2sync. This allows us to log the payloads that would otherwise have been sent to an instance of os2sync.
This log is saved as a text file which enables us to track changes to the output from the program on changes to the code. By assuming that the data isn't changing ("Kolding Kommune") we can track the changes to the file by running it locally after changing the code and then checking for changes in the log-file.

Ensure the image is build by running `docker-compose build`.
Then to run the exporter:
```docker-compose run --rm os2sync_export python -m os2sync_export```

Now running `git diff os2sync_requests.txt` will show any differences to payloads that would have been sent to os2sync.

*This only tests one default set of configurations, and only on the default Kolding dataset*