# Evergreen LibraryIQ Export

This software extracts data from an Evergreen server and securely transfers the output to a specified SFTP server. It also sends an email notification upon completion, indicating success or failure. The output data is stored locally in a specified archive folder.

This project connects [Evergreen ILS](https://evergreen-ils.org/), an open-source integrated library system, with [Library IQ](https://www.libraryiq.com/), a data analytics and visualization platform for libraries. By bridging these two systems, libraries can leverage Library IQ's powerful analytics tools to gain insights from their Evergreen data.

![Perl](https://img.shields.io/badge/Perl-39457E?style=for-the-badge&logo=perl&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)

## Features

- **Modular Design**: The script is divided into multiple modules for better maintainability and readability.
- **Chunking Data**: Large queries are processed in chunks to prevent memory consumption and timeouts.
- **Email Notifications**: Notifies staff of success or failure, including logs or summaries.
- **SFTP Transfer**: Securely uploads results to a remote server.
- **Logging**: Verbose logging to track the execution process and any errors.
- **History Tracking**: Stores the last run time to determine whether to run a partial (incremental) or full extract.

## Directory Structure

```
📁 archive/
    ├── 📄 .gitkeep
📁 config/
    └── ⚙️ library_config.conf.example
📁 lib/
    ├── 🐪 DBUtils.pm
    ├── 🐪 Email.pm
    ├── 🐪 Logging.pm   
    ├── 🐪 Queries.pm
    ├── 🐪 SFTP.pm
    └── 🐪 Utils.pm
📁 tmp/
    ├── 📄 .gitkeep
📄 .gitignore
🐪 email_test.pl
🐪 sftp_test.pl
🐪 extract_libraryiq.pl
📄 CONFIG.md
📄 README.md
```

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/Bibliomation-Inc/library-iq-extract
    cd libraryiq-extract
    ```

2. Copy the example configuration file and edit it to match your environment:
    ```bash
    cp config/library_config.conf.example config/library_config.conf
    vi config/library_config.conf
    ```

    For detailed configuration options, command line parameters, and automation setup, see [CONFIG.md](CONFIG.md).

3. Install the required Perl modules:
    ```bash
    cpan install DBI DBD::Pg Net::SFTP::Foreign Email::MIME Email::Sender::Simple
    ```

## Usage

Make sure the script has execute permissions:

```bash
chmod +x extract_libraryiq.pl
```

Run the script with the desired options:

```bash
./extract_libraryiq.pl --config config/library_config.conf
```

Run the script without any network operations (email, SFTP):

```bash
./extract_libraryiq.pl --config config/library_config.conf --no-email --no-sftp
```

A common configuration used for testing with libraryiq:

```bash
./extract_libraryiq.pl --config config/library_config.conf --full --debug --no-update-history
```

For a complete list of command line options, see [CONFIG.md](CONFIG.md#command-line-options).

## Process Flow

```mermaid
flowchart LR
    A[Start] --> B[Setup]
    B --> C[Process Data]
    C --> D[Create tar.gz Archive]
    D --> E[Send Email Notification]
    E --> F[Update Last Run Time & Cleanup]
    F --> G[Finish]
```

## Cleanup Strategy

The script automatically manages disk space by cleaning up files after each run:

- **Temporary Directory (`tempdir`)**: All files are deleted after each run, regardless of success or failure.
- **Archive Directory (`archive`)**: When `cleanup=1` in the config, only the most recent full extract and most recent diff extract are kept. Older files are automatically deleted.

### ⚠️ Warning: Temporary Directory Deletion

The directory specified in `tempdir` is **completely emptied** after every run. All files in this directory will be deleted, whether created by the script or not.

To avoid data loss:
- Use the default `tmp` directory (recommended)
- If using a custom path, ensure it's dedicated solely to this script.

- `tempdir`: Temporary files will be stored in the `tmp` directory relative to the script's location.
- `archive`: Final output files will be stored in the `archive` directory relative to the script's location.

If you need to use absolute paths, ensure that the `tempdir` points to a dedicated directory:

```plaintext
tempdir = /path/to/dedicated/tempdir
archive = /path/to/archive
```

## Acknowledgments

This project began as a fork of the [evergreen-libraryiq-export](https://github.com/mcoia/evergreen-libraryiq-export) repository by Blake ([@bmagic007](https://github.com/bmagic007)) from the [Mobius Consortium](https://github.com/mcoia). While this repository no longer shares commit history with the original, we want to express our gratitude for the foundational work that made this project possible.

Thank you to Blake and the Mobius team for starting this project!
