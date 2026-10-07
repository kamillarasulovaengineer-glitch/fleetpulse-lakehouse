# Databricks notebook source
# Applies versioned DDL from src/migrations to the target schema.
# Files are named V<version>__<description>.sql and each one runs exactly once, in version order.
# Applied versions are recorded in _schema_migrations with a checksum, so a file that has already
# reached an environment can't be edited quietly - the fix is always a new migration.

import hashlib
import re
from pathlib import Path

dbutils.widgets.text("catalog", "telematics")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("migrations_dir", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
migrations_dir = dbutils.widgets.get("migrations_dir")
if not schema or not migrations_dir:
    raise ValueError("schema and migrations_dir are required")

# COMMAND ----------

FILENAME = re.compile(r"V(\d+)__(\w+)\.sql")


def load_migrations(directory):
    found = []
    for path in Path(directory).iterdir():
        match = FILENAME.fullmatch(path.name)
        if match:
            sql = path.read_text().replace("\r\n", "\n")
            found.append((int(match.group(1)), match.group(2), sql))

    versions = [version for version, _, _ in found]
    if len(versions) != len(set(versions)):
        raise ValueError(f"duplicate migration versions in {directory}")
    return sorted(found)


def split_statements(sql):
    # Whole-line comments are dropped first. Good enough for DDL as long as no string literal
    # contains a semicolon.
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    return [statement.strip() for statement in body.split(";") if statement.strip()]


# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

spark.sql("""
    CREATE TABLE IF NOT EXISTS _schema_migrations (
        version     INT       NOT NULL,
        name        STRING    NOT NULL,
        checksum    STRING    NOT NULL,
        applied_at  TIMESTAMP NOT NULL
    )
""")

applied = {row.version: row.checksum for row in spark.table("_schema_migrations").collect()}
migrations = load_migrations(migrations_dir)
if not migrations:
    raise RuntimeError(f"no migrations found in {migrations_dir}")

for version, name, sql in migrations:
    checksum = hashlib.sha256(sql.encode()).hexdigest()

    if version in applied:
        if applied[version] != checksum:
            raise RuntimeError(f"V{version} was changed after it was applied; add a new migration instead")
        continue

    print(f"applying V{version}__{name}")
    for statement in split_statements(sql):
        spark.sql(statement)
    spark.sql(
        "INSERT INTO _schema_migrations VALUES (:version, :name, :checksum, current_timestamp())",
        args={"version": version, "name": name, "checksum": checksum},
    )

print(f"{catalog}.{schema} is at V{migrations[-1][0]}")
