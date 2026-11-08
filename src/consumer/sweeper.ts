import type { Config } from "../config.ts";
import type { Db } from "../db/db.ts";
import { laneHash, type Logger } from "../observability/log.ts";
import type { Metrics } from "../observability/metrics.ts";
import type { CooldownStore } from "../queue/cooldown.ts";
import { archiveExhausted, deadCounts, preserveDead, retentionSweep } from "../queue/dlq.ts";
import { type QueueDepth, reapExpiredLeases } from "../queue/store.ts";

/**
 * Periodic DB jobs. `fast` (every 5 s): lease reaper, cool-down skip
 * accounting, queue gauges. `slow` (SWEEP_INTERVAL_MS): DLQ preservation and
 * archive (§8), retention.
 */
export class Sweeper {
  private timers: ReturnType<typeof setInterval>[] = [];
  private busy = { fast: false, slow: false };

  constructor(
    private readonly deps: { db: Db; config: Config; logger: Logger; metrics: Metrics; cooldowns: CooldownStore; depth: QueueDepth },
  ) {}

  start(): void {
    this.timers.push(setInterval(() => void this.guard("fast", () => this.fast()), 5000));
    this.timers.push(setInterval(() => void this.guard("slow", () => this.slow()), this.deps.config.SWEEP_INTERVAL_MS));
  }

  stop(): void {
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
  }

  private async guard(kind: "fast" | "slow", fn: () => Promise<unknown>): Promise<void> {
    if (this.busy[kind]) return;
    this.busy[kind] = true;
    // Bounded: the pool's statement_timeout ends the query server-side; this releases the guard
    // even if a call never settles, so a stuck run cannot stop lease reaping for good.
    const limitMs = kind === "fast" ? 60_000 : Math.max(120_000, this.deps.config.SWEEP_INTERVAL_MS * 2);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        fn(),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error("sweep_timeout")), limitMs);
        }),
      ]);
    } catch (error) {
      this.deps.logger.event("sweeper_error", { area: "sweeper", kind, errorName: error instanceof Error ? error.name : "unknown" });
    } finally {
      clearTimeout(timer);
      this.busy[kind] = false;
    }
  }

  async fast(): Promise<void> {
    const { db, config, metrics, logger, cooldowns, depth } = this.deps;
    const reaped = await reapExpiredLeases(db, config.MAX_ATTEMPTS);
    if (reaped.requeued + reaped.dead > 0) logger.event("lease_reaped", { area: "sweeper", ...reaped });
    for (const s of await cooldowns.skippedLanes()) {
      metrics.inc("cooldown_skips_total", { provider: s.provider.split("-")[0] ?? s.provider }, s.queued);
      logger.event("forward_cooldown_skip", {
        area: "consumer",
        laneHash: laneHash(s.lane),
        provider: s.provider,
        queued: s.queued,
        cooldownRemainingSeconds: Math.ceil(s.remaining),
      });
    }
    metrics.set("cooldown_lanes", (await cooldowns.list()).length);
    depth.invalidate();
    metrics.set("envelopes_queued", await depth.global());
    metrics.set("queue_oldest_age_seconds", await depth.oldestAgeSeconds());
    metrics.replaceGauge(
      "lane_queued",
      (await depth.topLanes(10)).map((l) => ({ labels: { lane_hash: laneHash(l.lane) }, value: l.queued })),
    );
  }

  async slow(): Promise<void> {
    const { db, config, metrics, logger } = this.deps;
    for (const p of await preserveDead(db, config.DLQ_MAX_PRESERVATION_CYCLES)) {
      metrics.inc("dlq_preserved_total");
      logger.event("dlq_preserve", { area: "dlq", envelopeId: p.id, laneHash: laneHash(p.lane), cycle: p.cycle, delay: p.delay });
    }
    for (const a of await archiveExhausted(db, config.DLQ_MAX_PRESERVATION_CYCLES)) {
      metrics.inc("dlq_exhausted_total", { provider: a.provider.split("-")[0] ?? a.provider });
      logger.event("dlq_exhausted", {
        area: "dlq",
        alert: "webhook_dlq_preservation_exhausted",
        severity: 9,
        envelopeId: a.id,
        laneHash: laneHash(a.lane),
        provider: a.provider,
        cycle: a.cycle,
      });
    }
    const swept = await retentionSweep(db, {
      envelopeRetentionSeconds: config.ENVELOPE_RETENTION_SECONDS,
      archiveRetentionSeconds: config.DLQ_ARCHIVE_RETENTION_SECONDS,
    });
    if (swept.envelopes + swept.claims + swept.archive + swept.blobs > 0) logger.event("retention_sweep", { area: "sweeper", ...swept });
    const counts = await deadCounts(db);
    metrics.set("dlq_rows", counts.dead, { state: "dead" });
    metrics.set("dlq_rows", counts.archived, { state: "archived" });
  }
}
