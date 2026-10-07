# Databricks notebook source

import hashlib
import re
from pathlib import Path

dbutils.widgets.text("catalog", "telematics")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("migrations_dir", "")

# COMMAND ----------

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
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    return [statement.strip() for statement in body.split(";") if statement.strip()]


ADD_COLUMN = re.compile(r"ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMNS?\b", re.IGNORECASE)


def run_statement(statement):
    try:
        spark.sql(statement)
    except Exception as e:
        # an earlier run added the column but died before recording the version
        if ADD_COLUMN.match(statement) and "ALREADY_EXISTS" in str(e):
            print(f"  column already there, skipping: {statement.splitlines()[0]}")
            return
        raise


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

applied_now = []
for version, name, sql in migrations:
    checksum = hashlib.sha256(sql.encode()).hexdigest()

    if version in applied:
        if applied[version] != checksum:
            raise RuntimeError(f"V{version} was changed after it was applied; add a new migration instead")
        continue

    print(f"applying V{version}__{name}")
    for statement in split_statements(sql):
        run_statement(statement)
    spark.sql(
        "INSERT INTO _schema_migrations VALUES (:version, :name, :checksum, current_timestamp())",
        args={"version": version, "name": name, "checksum": checksum},
    )
    applied_now.append(f"V{version}__{name}")

dbutils.notebook.exit(
    f"{catalog}.{schema} at V{migrations[-1][0]}; applied this run: {', '.join(applied_now) or 'nothing'}"
)
