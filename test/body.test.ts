import assert from "node:assert/strict";
import { request } from "node:http";
import { describe, it } from "node:test";
import { BodyTooLargeError, readCappedBody } from "../src/ingress/body.ts";
import { listen } from "./support/harness.ts";

describe("readCappedBody", () => {
  it("reads within the cap, refuses a declared length over it, and stops a streamed body at the cap", async () => {
    const results: string[] = [];
    let maxBuffered = 0;
    const srv = await listen(async (req, res) => {
      try {
        const b = await readCappedBody(req, 1024);
        maxBuffered = Math.max(maxBuffered, b.length);
        results.push(`ok:${b.length}`);
      } catch (e) {
        results.push(e instanceof BodyTooLargeError ? "too_large" : "error");
      }
      res.writeHead(200, { connection: "close" });
      res.end();
    });
    const send = (body: Buffer, chunked: boolean) =>
      new Promise<void>((resolve) => {
        const r = request(srv.url, { method: "POST", headers: chunked ? { "transfer-encoding": "chunked" } : { "content-length": String(body.length) } }, (res) => {
          res.resume();
          res.on("end", resolve);
        });
        r.on("error", () => resolve());
        if (chunked) {
          for (let i = 0; i < body.length; i += 256) r.write(body.subarray(i, i + 256));
        } else r.write(body);
        r.end();
      });
    await send(Buffer.alloc(1024, 1), false);
    await send(Buffer.alloc(4096, 1), false);
    await send(Buffer.alloc(8192, 1), true);
    await srv.close();
    assert.deepEqual(results, ["ok:1024", "too_large", "too_large"]);
    assert.ok(maxBuffered <= 1024);
  });
});
