# Lab 08 — Iceberg

The Python script was reconstructed from my interactive command history.
The original execution outputs were not saved, and the S3 warehouse was
deleted during cleanup. This repository therefore preserves the code,
not a verified execution log.

## Observations and explanations

- Data rows are stored in Parquet files under data/.
- The metadata directory contains schemas, snapshots and manifests.
- An append creates a snapshot which references existing and new files.
  It does not need to copy the entire table.
- VERSION AS OF reads the files referenced by a particular snapshot.
  Time travel fails if required metadata or data files have been deleted.
- Adding a column changes table metadata without rewriting existing files.
  Existing rows return NULL for the new column unless a default applies.
- Users and orders follow the same data/ and metadata/ layout.
  Their file counts depend on partitions and write operations.
- A plain Parquet directory has no table-level snapshot history.
  To replace it safely, write a new directory and coordinate the switch
  for readers rather than overwriting files they are currently reading.
- Iceberg publishes a committed table state through an atomic metadata
  commit, relying on the catalog and storage's commit guarantees.
- Parquet is a columnar file format.
- Iceberg is a table format providing snapshots, schema evolution and
  transactional table changes.
- An Iceberg catalog resolves table names to metadata and coordinates
  table discovery and commits across engines.

## Reproduction

Configure Spark with the Iceberg catalog and valid S3 credentials.
Restore the silver tables from Lab 07 before running the script.
The script appends synthetic data, so rerunning it adds more rows.
