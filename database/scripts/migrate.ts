
import { Client } from "pg";
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";

const migrationDir = path.resolve(process.cwd(), "migrations");
const statusOnly = process.argv.includes("--status");

const lockId = 72419381;

const HISTORY_TABLE = "hip.schema_migrations";

type Migration = {
  version: string;
  filename: string;
  sql: string;
  checksum: string;
};

type AppliedMigration = {
  version: string;
  checksum: string;
};

async function loadMigrations(): Promise<Migration[]> {
  const entries = await readdir(migrationDir);

  const filenames = entries
    .filter((name) => name.endsWith(".sql"))
    .sort();

  const migrations: Migration[] = [];
  const versions = new Set<string>();

  for (const filename of filenames) {
    if (!/^\d+_[a-z0-9_]+\.sql$/.test(filename)) {
      throw new Error(`Invalid migration filename: ${filename}`);
    }

    const version = filename.replace(/\.sql$/, "");

    if (versions.has(version)) {
      throw new Error(`Duplicate migration: ${version}`);
    }

    versions.add(version);

    const sql = await readFile(
      path.join(migrationDir, filename),
      "utf8"
    );

    if (!sql.trim()) {
      throw new Error(`Empty migration: ${filename}`);
    }

    const checksum = createHash("sha256")
      .update(sql)
      .digest("hex");

    migrations.push({
      version,
      filename,
      sql,
      checksum,
    });
  }

  if (
    migrations.length === 0 ||
    migrations[0].version !== "0001_schema_migrations"
  ) {
    throw new Error(
      "Required bootstrap migration 0001_schema_migrations.sql is missing."
    );
  }

  return migrations;
}

function createDatabaseClient(): Client {
  const password = process.env.HIP_DB_PASSWORD;

  if (!password) {
    throw new Error(
      "HIP_DB_PASSWORD environment variable is required."
    );
  }

  const port = Number(process.env.HIP_DB_PORT || "5432");

  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("Invalid HIP_DB_PORT.");
  }

  return new Client({
    host: process.env.HIP_DB_HOST || "localhost",
    port,
    database: process.env.HIP_DB_NAME || "hip_db",
    user: process.env.HIP_DB_USER || "hip_migrator",
    password,
    connectionTimeoutMillis: 10000,
    query_timeout: 60000,
    application_name: "hip-migration-runner",
  });
}

async function migrationTableExists(
  client: Client
): Promise<boolean> {
  const result = await client.query(
    "SELECT to_regclass($1) AS table_name",
    [HISTORY_TABLE]
  );

  return result.rows[0]?.table_name !== null;
}

async function getAppliedMigrations(
  client: Client
): Promise<Map<string, string>> {
  const applied = new Map<string, string>();

  if (!(await migrationTableExists(client))) {
    return applied;
  }

  const result = await client.query<AppliedMigration>(
    `SELECT version, checksum
     FROM hip.schema_migrations
     ORDER BY version`
  );

  for (const row of result.rows) {
    applied.set(row.version, row.checksum);
  }

  return applied;
}

function validateMigrationHistory(
  migrations: Migration[],
  applied: Map<string, string>
): void {
  const known = new Map(
    migrations.map((migration) => [
      migration.version,
      migration,
    ])
  );

  for (const [version, checksum] of applied) {
    const migration = known.get(version);

    if (!migration) {
      throw new Error(
        `Applied migration missing from disk: ${version}`
      );
    }

    if (migration.checksum !== checksum) {
      throw new Error(
        `Migration checksum mismatch: ${version}`
      );
    }
  }

  const appliedVersions = [...applied.keys()];
  const expectedPrefix = migrations
    .slice(0, appliedVersions.length)
    .map((migration) => migration.version);

  if (
    appliedVersions.length !== expectedPrefix.length ||
    appliedVersions.some(
      (version, index) => version !== expectedPrefix[index]
    )
  ) {
    throw new Error(
      "Migration history is not a valid ordered prefix."
    );
  }
}

async function applyMigration(
  client: Client,
  migration: Migration
): Promise<void> {
  console.log(`Applying ${migration.filename}`);

  await client.query("BEGIN");

  const start = performance.now();

  try {
    await client.query(
      "SET LOCAL lock_timeout = '10s'"
    );

    await client.query(
      "SET LOCAL statement_timeout = '60s'"
    );

    await client.query(migration.sql);

    const executionMs = Math.max(
      0,
      Math.round(performance.now() - start)
    );

    await client.query(
      `INSERT INTO hip.schema_migrations
       (version, checksum, execution_ms)
       VALUES ($1, $2, $3)`,
      [
        migration.version,
        migration.checksum,
        executionMs,
      ]
    );

    await client.query("COMMIT");

    console.log(
      `Applied ${migration.filename} (${executionMs} ms)`
    );
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch (rollbackError) {
      console.error(
        "Migration rollback failed:",
        rollbackError
      );
    }

    throw error;
  }
}

async function main(): Promise<void> {
  const migrations = await loadMigrations();
  const client = createDatabaseClient();

  let connected = false;
  let lockAcquired = false;

  try {
    await client.connect();
    connected = true;

        const lockDeadline = Date.now() + 30000;

    while (!lockAcquired && Date.now() < lockDeadline) {
      const result = await client.query(
        "SELECT pg_try_advisory_lock($1) AS acquired",
        [lockId]
      );

      lockAcquired = result.rows[0]?.acquired === true;

      if (!lockAcquired) {
        await new Promise<void>((resolve) =>
          setTimeout(resolve, 500)
        );
      }
    }

    if (!lockAcquired) {
      throw new Error(
        "Migration lock unavailable after 30 seconds."
      );
    }

    const applied = await getAppliedMigrations(client);

    validateMigrationHistory(migrations, applied);

    if (statusOnly) {
      console.log("\nHIP DATABASE MIGRATION STATUS\n");

      for (const migration of migrations) {
        const status = applied.has(migration.version)
          ? "APPLIED"
          : "PENDING";

        console.log(
          `${status.padEnd(8)} ${migration.filename}`
        );
      }

      console.log(
        `\nTotal: ${migrations.length}, ` +
        `Applied: ${applied.size}, ` +
        `Pending: ${migrations.length - applied.size}`
      );

      return;
    }

    for (const migration of migrations) {
      if (applied.has(migration.version)) {
        console.log(
          `Skipping ${migration.filename} (already applied)`
        );

        continue;
      }

      await applyMigration(client, migration);
    }

    console.log(
      "\nHIP database migrations completed successfully."
    );
  } finally {
    if (connected) {
      try {
        if (lockAcquired) {
          await client.query(
            "SELECT pg_advisory_unlock($1)",
            [lockId]
          );
        }
      } finally {
        await client.end();
      }
    }
  }
}

main().catch((error: unknown) => {
  const message =
    error instanceof Error
      ? error.message
      : String(error);

  console.error("\nHIP migration error:", message);

  process.exitCode = 1;
});
