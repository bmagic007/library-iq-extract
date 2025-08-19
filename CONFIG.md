# Configuration and Setup Guide

This document contains detailed configuration options, command line parameters, module descriptions, and automation setup for the Evergreen LibraryIQ Export tool.

## Configuration File

Edit the `config/library_config.conf` file to set the appropriate values for your environment. Key configuration options include:

| Configuration Option | Description                                                   |
| -------------------- | ------------------------------------------------------------- |
| `logfile`            | Path to the log file.                                         |
| `tempdir`            | Temporary directory for storing intermediate files.           |
| `archive`            | Directory for storing archived files.                         |
| `cleanup`            | Whether to clean up the archive directory.                    |
| `diff_overlap_days`  | Number of days to overlap when calculating incremental data.  |
| `librarynames`       | Comma-separated list of branch/system shortnames.             |
| `chunksize`          | Number of records to process per chunk.                       |
| `ftphost`            | SFTP server hostname.                                         |
| `ftplogin`           | SFTP server login username.                                   |
| `ftppass`            | SFTP server login password.                                   |
| `remote_directory`   | Directory on the SFTP server where files will be uploaded.    |
| `alwaysemail`        | Always send email notifications, even if there are no errors. |
| `fromemail`          | Email address from which notifications will be sent.          |
| `erroremaillist`     | Comma-separated list of email addresses to notify on error.   |
| `successemaillist`   | Comma-separated list of email addresses to notify on success. |

## Command Line Options

| Option                | Description                                                    |
| --------------------- | -------------------------------------------------------------- |
| `--config`            | Path to the configuration file (default: library_config.conf). |
| `--debug`             | Enable debug mode for more verbose output.                     |
| `--full`              | Perform a full dataset extraction.                             |
| `--no-email`          | Disable email notifications.                                   |
| `--no-sftp`           | Disable SFTP file transfer.                                    |
| `--drop-history`      | Drop and recreate the libraryiq schema before running.         |
| `--no-update-history` | Do not update the last run time in the history table.          |

## Perl Modules

| Module         | Description                                                                                        |
| -------------- | -------------------------------------------------------------------------------------------------- |
| **DBUtils.pm** | Handles database connections and chunked queries.                                                  |
| **Email.pm**   | Handles email notifications.                                                                       |
| **Logging.pm** | Handles logging with timestamps.                                                                   |
| **Queries.pm** | Contains SQL queries for fetching data.                                                            |
| **SFTP.pm**    | Handles SFTP file transfers.                                                                       |
| **Utils.pm**   | Contains utility functions for reading configuration, tracking history, and processing data types. |

## Setting Up Cron Jobs

To automate the extraction process, you can set up cron jobs to run the script at specified intervals. For example, you can run a full extract once per month and an incremental extract nightly.

1. Open the crontab file for editing:
    ```bash
    crontab -e
    ```

2. Add the following lines to schedule the full extract to run at 2 AM on the first day of every month and the incremental extract to run at 2 AM every night:

    ```bash
    # Full extract on the first day of every month at 2 AM
    0 2 1 * * /path/to/extract_libraryiq.pl --config /path/to/config/library_config.conf --full

    # Incremental extract every night at 2 AM
    0 2 * * * /path/to/extract_libraryiq.pl --config /path/to/config/library_config.conf
    ```

    Replace `/path/to/extract_libraryiq.pl` and `/path/to/config/library_config.conf` with the actual paths to your script and configuration file.

3. Save and close the crontab file.

These cron jobs will ensure that the full extract runs once a month and the incremental extract runs nightly, automating the data extraction process.
