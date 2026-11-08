type Labels = Record<string, string | number>;

function labelKey(labels: Labels): string {
  const keys = Object.keys(labels).sort();
  return keys.map((k) => `${k}="${String(labels[k]).replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n")}"`).join(",");
}

const BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];

/**
 * Minimal Prometheus registry (no dependency). Counter and gauge label sets
 * are bounded by construction: lanes are only ever exported as top-N hashes.
 */
export class Metrics {
  private counters = new Map<string, Map<string, number>>();
  private gauges = new Map<string, Map<string, number>>();
  private histograms = new Map<string, Map<string, { buckets: number[]; sum: number; count: number }>>();
  readonly prefix = "relayfile_control_";

  inc(name: string, labels: Labels = {}, by = 1): void {
    const series = this.counters.get(name) ?? new Map<string, number>();
    const key = labelKey(labels);
    series.set(key, (series.get(key) ?? 0) + by);
    this.counters.set(name, series);
  }

  set(name: string, value: number, labels: Labels = {}): void {
    const series = this.gauges.get(name) ?? new Map<string, number>();
    series.set(labelKey(labels), value);
    this.gauges.set(name, series);
  }

  /** Replace every series of a gauge (used for top-N lane gauges). */
  replaceGauge(name: string, values: { labels: Labels; value: number }[]): void {
    const series = new Map<string, number>();
    for (const v of values) series.set(labelKey(v.labels), v.value);
    this.gauges.set(name, series);
  }

  observe(name: string, seconds: number, labels: Labels = {}): void {
    const series = this.histograms.get(name) ?? new Map();
    const key = labelKey(labels);
    const h = series.get(key) ?? { buckets: BUCKETS.map(() => 0), sum: 0, count: 0 };
    BUCKETS.forEach((b, i) => {
      if (seconds <= b) h.buckets[i] = (h.buckets[i] ?? 0) + 1;
    });
    h.sum += seconds;
    h.count += 1;
    series.set(key, h);
    this.histograms.set(name, series);
  }

  counter(name: string, labels: Labels = {}): number {
    return this.counters.get(name)?.get(labelKey(labels)) ?? 0;
  }

  /** Sum of a counter across all label sets matching `match`. */
  counterSum(name: string, match: Labels = {}): number {
    const want = Object.entries(match).map(([k, v]) => `${k}="${v}"`);
    let total = 0;
    for (const [key, value] of this.counters.get(name) ?? []) {
      if (want.every((w) => key.split(",").includes(w))) total += value;
    }
    return total;
  }

  gauge(name: string, labels: Labels = {}): number | undefined {
    return this.gauges.get(name)?.get(labelKey(labels));
  }

  render(): string {
    const lines: string[] = [];
    const fmt = (name: string, key: string, value: number) => `${this.prefix}${name}${key ? `{${key}}` : ""} ${value}`;
    for (const [name, series] of this.counters) {
      lines.push(`# TYPE ${this.prefix}${name} counter`);
      for (const [key, value] of series) lines.push(fmt(name, key, value));
    }
    for (const [name, series] of this.gauges) {
      lines.push(`# TYPE ${this.prefix}${name} gauge`);
      for (const [key, value] of series) lines.push(fmt(name, key, value));
    }
    for (const [name, series] of this.histograms) {
      lines.push(`# TYPE ${this.prefix}${name} histogram`);
      for (const [key, h] of series) {
        BUCKETS.forEach((b, i) => {
          const le = `le="${b}"`;
          lines.push(`${this.prefix}${name}_bucket{${key ? `${key},` : ""}${le}} ${h.buckets[i]}`);
        });
        lines.push(`${this.prefix}${name}_bucket{${key ? `${key},` : ""}le="+Inf"} ${h.count}`);
        lines.push(`${this.prefix}${name}_sum${key ? `{${key}}` : ""} ${h.sum}`);
        lines.push(`${this.prefix}${name}_count${key ? `{${key}}` : ""} ${h.count}`);
      }
    }
    return lines.join("\n") + "\n";
  }
}
