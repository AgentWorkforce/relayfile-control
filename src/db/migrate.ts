import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Db } from "./db.ts";

/** Advisory lock id shared by every relayfile-control migrator. */
const MIGRATION_LOCK_ID = 0x72666374; // "rfct"

export const DEFAULT_MIGRATIONS_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../migrations");

export async function listMigrations(dir = DEFAULT_MIGRATIONS_DIR): Promise<{ version: string; file: string }[]> {
  const files = (await readdir(dir)).filter((name) => /^\d{4}_[a-z0-9_]+\.sql$/.test(name)).sort();
  const seen = new Set<string>();
  for (const file of files) {
    const version = file.slice(0, 4);
    if (seen.has(version)) throw new Error(`duplicate migration version ${version}`);
    seen.add(version);
  }
  return files.map((file) => ({ version: file.slice(0, 4), file: path.join(dir, file) }));
}

/**
 * Apply pending migrations in order, each in its own transaction, under a
 * session advisory lock so concurrently starting pods never race.
 */
export async function migrate(db: Db, dir = DEFAULT_MIGRATIONS_DIR): Promise<string[]> {
  const applied: string[] = [];
  await db.tx(async (q) => {
    // Lock first: even `CREATE TABLE IF NOT EXISTS` races between concurrent migrators (23505 on pg_type).
    await q.query("SELECT pg_advisory_xact_lock($1)", [MIGRATION_LOCK_ID]);
    await q.query("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())");
    const done = new Set((await q.query<{ version: string }>("SELECT version FROM schema_migrations")).rows.map((r) => r.version));
    for (const migration of await listMigrations(dir)) {
      if (done.has(migration.version)) continue;
      const sql = await readFile(migration.file, "utf8");
      await q.exec(sql);
      await q.query("INSERT INTO schema_migrations (version) VALUES ($1)", [migration.version]);
      applied.push(migration.version);
    }
  });
  return applied;
}

/** Highest applied version and whether every shipped migration is applied. */
export async function migrationStatus(db: Db, dir = DEFAULT_MIGRATIONS_DIR): Promise<{ current: string | null; latest: string | null; upToDate: boolean }> {
  const shipped = await listMigrations(dir);
  const latest = shipped.at(-1)?.version ?? null;
  let current: string | null = null;
  try {
    const { rows } = await db.query<{ version: string | null }>("SELECT max(version) AS version FROM schema_migrations");
    current = rows[0]?.version ?? null;
  } catch {
    current = null;
  }
  return { current, latest, upToDate: current !== null && current === latest };
}
