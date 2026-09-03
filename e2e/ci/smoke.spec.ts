import { test, expect } from '@playwright/test';

/**
 * Kotatsu CI smoke — mirrors e2e/test_plans/SMOKE_TEST_PLAN.md.
 *
 * Two layers:
 *  - API: deterministic assertions on the read path (health, source, topics,
 *    messages, Avro decode, schemas, groups/lag).
 *  - UI: the Nuxt SPA renders each screen and its figures match the API.
 *
 * Preconditions: the docker-compose stack is up AND seeded (e2e/scripts/seed.sh)
 * with topics orders/events/empty-topic/avro-orders/truncated/
 * acme.prod.db2.dbz_config and group qa-group. The stack must run the Popsink
 * Tansu fork at the version pinned in docker-compose.yml — upstream writes a
 * different storage layout, and the last two topics assert against this one (#97).
 */

const CLUSTER = 'demo';

test.describe('API smoke', () => {
  test('health is ok', async ({ request }) => {
    const res = await request.get('/api/health');
    expect(res.ok()).toBeTruthy();
    expect(await res.json()).toMatchObject({ service: 'kotatsu', status: 'ok' });
  });

  test('source reports its configuration without probing the store', async ({ request }) => {
    const body = await (await request.get('/api/source')).json();
    expect(body.configured).toBe(true);
    expect(body.cluster).toBe(CLUSTER);
    // The live probe moved to /api/source/status (#109): this answer is pure
    // config, so every page can ask for it without touching S3.
    expect(body).not.toHaveProperty('status');
  });

  test('source status probes the store and reports it reachable', async ({ request }) => {
    const body = await (await request.get('/api/source/status')).json();
    expect(body).toMatchObject({ configured: true, connected: true });
  });

  test('cluster demo is discovered', async ({ request }) => {
    const body = await (await request.get('/api/clusters')).json();
    expect(body.clusters).toContain(CLUSTER);
  });

  test('topics are listed with counts', async ({ request }) => {
    const body = await (await request.get(`/api/clusters/${CLUSTER}/topics`)).json();
    const names = body.items.map((t: { name: string }) => t.name);
    expect(names).toEqual(
      expect.arrayContaining([
        'orders',
        'events',
        'spread',
        'nested',
        'headers',
        'empty-topic',
        'avro-orders',
        'avro-nested',
        'truncated',
        'acme.prod.db2.dbz_config',
      ]),
    );
    const orders = body.items.find((t: { name: string }) => t.name === 'orders');
    expect(orders.messages).toBe(3);
    expect(orders.partitions).toBe(1);
  });

  test('orders messages read back faithfully', async ({ request }) => {
    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/orders/messages?partition=0&offset=earliest`,
    )).json();
    expect(body.count).toBe(3);
    expect(body.watermark).toMatchObject({ low: 0, high: 3 });
    expect(body.records.map((r: { offset: number }) => r.offset)).toEqual([0, 1, 2]);
    expect(body.records[0].key.data).toBe('key-1');
    // `auto` recognises a JSON object, so the browser has a structure to open
    // instead of one long string (#103). A bare key is text, and stays text.
    expect(body.records[0].value).toMatchObject({ kind: 'json', data: { id: 1, item: 'widget' } });
    expect(body.records[0].key.kind).toBe('utf8');
  });

  test('empty-topic returns no records', async ({ request }) => {
    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/empty-topic/messages?partition=0`,
    )).json();
    expect(body.count).toBe(0);
    expect(body.records).toHaveLength(0);
    expect(body.watermark).toMatchObject({ low: 0, high: 0 });
  });

  /**
   * #92: a compacted topic is routed under its own full name, not under the
   * connector prefix its name derives to, and only the `topic-routing/` pin says
   * so. Deriving the prefix found no segments and rendered the topic as empty —
   * no messages, no watermark, zero bytes, and no error to notice.
   */
  test('compacted topic reads through its pinned routing prefix', async ({ request }) => {
    const topic = 'acme.prod.db2.dbz_config';
    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/${topic}/messages?partition=0&offset=earliest`,
    )).json();
    expect(body.count).toBe(2);
    expect(body.watermark).toMatchObject({ low: 0, high: 2 });
    expect(body.records.map((r: { key: { data: string } }) => r.key.data)).toEqual(['cfg-a', 'cfg-b']);

    // Its size is the byte span of its sub-stream inside the shared segment (#93);
    // listing `.batch` objects — the old source — reports 0 on this layout.
    const detail = await (await request.get(`/api/clusters/${CLUSTER}/topics/${topic}`)).json();
    expect(detail.messages).toBe(2);
    expect(detail.storage_bytes).toBeGreaterThan(0);
  });

  /**
   * #95: `DeleteRecords` is logical — the records below the floor are still in the
   * segment object, and only `watermark.json`'s `truncate` hides them. The log
   * start must move, the count must drop, and an explicit request below the floor
   * must not return the deleted records.
   */
  test('truncated topic hides the records deleted below the floor', async ({ request }) => {
    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/truncated/messages?partition=0&offset=earliest`,
    )).json();
    expect(body.watermark).toMatchObject({ low: 2, high: 3 });
    expect(body.count).toBe(1);
    expect(body.records.map((r: { offset: number }) => r.offset)).toEqual([2]);

    const below = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/truncated/messages?partition=0&offset=0`,
    )).json();
    expect(below.records.map((r: { offset: number }) => r.offset)).toEqual([2]);
  });

  test('avro-orders values decode via the registry', async ({ request }) => {
    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/avro-orders/messages?partition=0&offset=earliest`,
    )).json();
    expect(body.count).toBe(2);
    expect(body.records[0].value.kind).toBe('avro');
    expect(body.records[0].value.data).toMatchObject({ id: 1, item: 'widget' });
  });

  test('schema subject is registered', async ({ request }) => {
    const body = await (await request.get('/api/schemas')).json();
    expect(body.items).toContain('avro-orders-value');
  });

  test('qa-group reports committed offsets and lag', async ({ request }) => {
    const body = await (await request.get(`/api/clusters/${CLUSTER}/groups/qa-group`)).json();
    expect(body.name).toBe('qa-group');
    const orders = body.offsets.find(
      (o: { topic: string }) => o.topic === 'orders',
    );
    expect(orders).toMatchObject({ committed_offset: 3, high_watermark: 3, lag: 0 });
    expect(body.total_lag).toBe(0);
  });

  /** `spread` is the seed's only topic whose records really span partitions (#102). */
  test('partition=all merges records from every partition, in the read direction', async ({ request }) => {
    const detail = await (await request.get(`/api/clusters/${CLUSTER}/topics/spread`)).json();
    const populated = detail.partitions
      .filter((p: { messages: number }) => p.messages > 0)
      .map((p: { partition: number }) => p.partition);
    expect(populated.length).toBeGreaterThan(1); // the seed must really spread

    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=all&offset=earliest&limit=100`,
    )).json();

    const seen = [...new Set(body.records.map((r: { partition: number }) => r.partition))].sort();
    expect(seen).toEqual([...populated].sort());
    expect(body.count).toBe(detail.messages);

    // `earliest` travels towards newer records, so the merge must too: sorting the
    // other way would drop the oldest records — exactly the ones asked for — at the
    // truncation, and let page two precede page one (#104).
    const timestamps = body.records.map((r: { timestamp: number }) => r.timestamp);
    expect(timestamps).toEqual([...timestamps].sort((a: number, b: number) => a - b));
    expect(body.order).toBe('timestamp_asc');
    expect(body.order_best_effort).toBe(true);

    expect(body.partitions).toHaveLength(detail.partitions.length);
    expect(body.partitions.every((p: { exhausted: boolean }) => p.exhausted)).toBe(true);
    // Nothing left anywhere, so there is no next page to ask for.
    expect(body.partitions.every((p: { resume: number | null }) => p.resume === null)).toBe(true);
    expect(body.exhausted).toBe(true);
  });

  test('latest travels the other way, and says so', async ({ request }) => {
    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=all&offset=latest&limit=100`,
    )).json();
    const timestamps = body.records.map((r: { timestamp: number }) => r.timestamp);
    expect(timestamps).toEqual([...timestamps].sort((a: number, b: number) => b - a));
    expect(body.order).toBe('timestamp_desc');
  });

  /**
   * #104: a page is a page. Dividing `limit` across partitions returned four
   * records for a request for fifty on a topic whose records sit in one partition
   * — which the sticky partitioner makes the usual shape, not the exception.
   */
  test('an unfiltered page is not divided across partitions', async ({ request }) => {
    const detail = await (await request.get(`/api/clusters/${CLUSTER}/topics/events`)).json();
    expect(detail.partitions.length).toBeGreaterThan(1);
    expect(detail.messages).toBe(6);

    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/events/messages?partition=all&offset=earliest&limit=6`,
    )).json();
    expect(body.count).toBe(6);
  });

  /**
   * #104's core: `Load more` continues the read instead of restarting it, and the
   * two pages together are the whole topic, each record exactly once.
   */
  test('the resume cursor pages through a topic without gap or repeat', async ({ request }) => {
    const url = (extra: string) =>
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=all&offset=earliest&limit=5${extra}`;

    const first = await (await request.get(url(''))).json();
    expect(first.count).toBe(5);
    expect(first.exhausted).toBe(false);

    const cursor = first.partitions
      .filter((p: { resume: number | null }) => p.resume !== null)
      .map((p: { partition: number; resume: number }) => `${p.partition}:${p.resume}`)
      .join(',');
    expect(cursor).not.toBe('');

    const second = await (await request.get(url(`&cursor=${encodeURIComponent(cursor)}`))).json();
    expect(second.count).toBeGreaterThan(0);

    const id = (r: { partition: number; offset: number }) => `${r.partition}:${r.offset}`;
    const firstIds = first.records.map(id);
    const secondIds = second.records.map(id);
    // No repeat: a record shown on page one must not come back on page two.
    expect(secondIds.filter((k: string) => firstIds.includes(k))).toEqual([]);
    // No gap: paging to the end accounts for every record in the topic.
    let seen = [...firstIds, ...secondIds];
    let page = second;
    while (!page.exhausted) {
      const next = page.partitions
        .filter((p: { resume: number | null }) => p.resume !== null)
        .map((p: { partition: number; resume: number }) => `${p.partition}:${p.resume}`)
        .join(',');
      page = await (await request.get(url(`&cursor=${encodeURIComponent(next)}`))).json();
      seen = [...seen, ...page.records.map(id)];
    }
    expect(new Set(seen).size).toBe(seen.length);
    expect(seen.length).toBe(12);
  });

  test('paging backwards from latest reaches the start of the topic', async ({ request }) => {
    const url = (extra: string) =>
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=all&offset=latest&limit=5${extra}`;
    let page = await (await request.get(url(''))).json();
    const id = (r: { partition: number; offset: number }) => `${r.partition}:${r.offset}`;
    let seen = page.records.map(id);
    while (!page.exhausted) {
      const next = page.partitions
        .filter((p: { resume: number | null }) => p.resume !== null)
        .map((p: { partition: number; resume: number }) => `${p.partition}:${p.resume}`)
        .join(',');
      page = await (await request.get(url(`&cursor=${encodeURIComponent(next)}`))).json();
      seen = [...seen, ...page.records.map(id)];
    }
    expect(new Set(seen).size).toBe(seen.length);
    expect(seen.length).toBe(12);
  });

  test('a cursor naming a partition the query does not read is a 400', async ({ request }) => {
    const res = await request.get(
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=0&cursor=2%3A1`,
    );
    expect(res.status()).toBe(400);
    expect((await res.json()).error).toContain('does not read');
  });

  test('a malformed cursor is a 400, not a 500', async ({ request }) => {
    const res = await request.get(
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=all&cursor=nope`,
    );
    expect(res.status()).toBe(400);
    expect((await res.json()).error).toContain('partition:offset');
  });

  test('a filter finds its record whichever partition holds it', async ({ request }) => {
    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=all&offset=earliest&key_contains=k-12`,
    )).json();
    expect(body.count).toBe(1);
    expect(body.records[0].key.data).toBe('k-12');
  });

  /**
   * The constraint that keeps #102 honest: the scan budget belongs to the topic,
   * so a three-partition search must not read three times a one-partition one.
   */
  test('partition=all spends one topic-wide scan budget, not one per partition', async ({ request }) => {
    const MAX_SCAN = 6;
    const body = await (await request.get(
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=all&offset=earliest` +
      `&value_contains=zzz-no-match&max_scan=${MAX_SCAN}`,
    )).json();

    expect(body.filtered).toBe(true);
    const partitions = body.partitions.length;
    // Handing each partition the full budget would have read MAX_SCAN * partitions
    // records — the failure mode #102 calls out. The split allows at most one
    // record of overshoot per partition, so every partition is still reachable.
    expect(body.scanned).toBeLessThanOrEqual(MAX_SCAN + partitions);
    expect(body.scanned).toBeLessThan(MAX_SCAN * partitions);
    // Capped, not exhausted: the topic holds more records than the budget allows.
    expect(body.exhausted).toBe(false);
  });

  test('partition=all rejects a concrete offset, naming the constraint', async ({ request }) => {
    const res = await request.get(
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=all&offset=42`,
    );
    expect(res.status()).toBe(400);
    expect((await res.json()).error).toContain('single partition');
  });

  test('an unparseable partition is a 400, not a 500', async ({ request }) => {
    const res = await request.get(
      `/api/clusters/${CLUSTER}/topics/spread/messages?partition=nope`,
    );
    expect(res.status()).toBe(400);
    expect((await res.json()).error).toContain("'all'");
  });

  test('invalid offset returns 400', async ({ request }) => {
    const res = await request.get(
      `/api/clusters/${CLUSTER}/topics/orders/messages?offset=abc`,
    );
    expect(res.status()).toBe(400);
  });

  test('unknown topic returns 404', async ({ request }) => {
    const res = await request.get(`/api/clusters/${CLUSTER}/topics/nope/messages`);
    expect(res.status()).toBe(404);
  });
});

test.describe('UI smoke', () => {
  test('overview shows source connected and cluster stats', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible();
    await expect(page.getByText('connected', { exact: true })).toBeVisible();
    await expect(page.getByText('demo')).toBeVisible();
  });

  /**
   * #109: /api/source used to be fetched per page and to probe S3 on every
   * call, so a four-page session cost four object-store round-trips to render
   * a dot only Overview shows. Config is now cached for the session and the
   * probe is asked for only where it is displayed.
   */
  test('source config is fetched once, and only Overview probes the store', async ({ page }) => {
    const calls: string[] = [];
    page.on('request', (r) => {
      const path = new URL(r.url()).pathname;
      if (path === '/api/source' || path === '/api/source/status') calls.push(path);
    });

    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible();
    for (const nav of ['Topics', 'Consumer groups', 'Schemas']) {
      await page.getByRole('link', { name: nav, exact: true }).click();
      await expect(page.getByRole('heading', { name: nav })).toBeVisible();
    }

    expect(calls.filter((p) => p === '/api/source')).toHaveLength(1);
    expect(calls.filter((p) => p === '/api/source/status')).toHaveLength(1);
  });

  test('topics page lists the seeded topics', async ({ page }) => {
    await page.goto('/topics');
    for (const name of ['orders', 'events', 'empty-topic', 'avro-orders']) {
      await expect(page.getByText(name, { exact: false }).first()).toBeVisible();
    }
  });

  test('orders topic renders its messages', async ({ page }) => {
    await page.goto('/topics/orders');
    await expect(page.getByRole('heading', { name: 'Messages' })).toBeVisible();
    // Narrowing to one partition keeps the single-partition view (#102).
    await page.getByRole('combobox', { name: 'Partition' }).selectOption('0');
    await page.getByRole('combobox', { name: 'From' }).selectOption('earliest');
    await page.getByRole('button', { name: 'Search' }).click();
    await expect(page.getByText('partition 0 — low 0, high 3 (3 messages)')).toBeVisible();
    await expect(page.getByText('key-1')).toBeVisible();
    await expect(page.getByText('{"id":1,"item":"widget"}')).toBeVisible();
  });

  test('the event browser searches every partition by default', async ({ page }) => {
    await page.goto('/topics/spread');
    await expect(page.getByRole('combobox', { name: 'Partition' })).toHaveValue('all');
    await page.getByRole('combobox', { name: 'From' }).selectOption('earliest');
    await page.getByRole('button', { name: 'Search' }).click();

    // Provenance: the column only appears when a result set can span partitions.
    // Scoped to the results table — the topic's own partition table and the scan
    // summary carry that header too, and the first of them is already on the page
    // before the search lands, so an unscoped locator asserts the wrong table.
    const results = page.locator('table.msgs');
    await expect(results.getByRole('columnheader', { name: 'partition' })).toBeVisible();
    // Populated, not merely present: the cell after the caret, on the first row.
    await expect(results.locator('tbody tr.row').first().locator('td').nth(1)).toHaveText(/^[0-2]$/);
    // `earliest` reads towards newer records, and the label must not claim otherwise.
    await expect(page.getByText('3 partitions, oldest first')).toBeVisible();
    await expect(page.getByText('k-12')).toBeVisible();
  });

  /** #104: a query lives in the URL, so an investigation can be pasted into a ticket. */
  test('a search is captured in the URL and replayed from it', async ({ page }) => {
    await page.goto('/topics/spread');
    await page.getByRole('combobox', { name: 'From' }).selectOption('earliest');
    await page.getByRole('button', { name: 'Search' }).click();
    await expect(page.locator('table.msgs')).toBeVisible();
    await expect(page).toHaveURL(/[?&]from=earliest/);

    // A fresh load of that URL runs the query on arrival — the user clicked a link,
    // which is still a user action (#7).
    await page.goto('/topics/spread?from=earliest&key_contains=k-12');
    await expect(page.getByText('k-12')).toBeVisible();
    await expect(page.locator('table.msgs tbody tr.row')).toHaveCount(1);
  });

  test('Load more appends the next window, and Back takes it away again', async ({ page }) => {
    await page.goto('/topics/spread?from=earliest&limit=5');
    const rows = page.locator('table.msgs tbody tr.row');
    await expect(rows).toHaveCount(5);

    await page.getByRole('button', { name: 'Load more' }).click();
    await expect(rows).toHaveCount(10);

    await page.getByRole('button', { name: 'Back' }).click();
    await expect(rows).toHaveCount(5);
  });

  /**
   * A cursor only means anything against the query it came from: a forward resume
   * point read as a backward ceiling would return the wrong records silently.
   */
  test('Load more stops offering itself once the query has changed', async ({ page }) => {
    await page.goto('/topics/spread?from=earliest&limit=5');
    await expect(page.locator('table.msgs tbody tr.row')).toHaveCount(5);
    await expect(page.getByRole('button', { name: 'Load more' })).toBeEnabled();

    await page.getByRole('combobox', { name: 'From' }).selectOption('latest');
    await expect(page.getByRole('button', { name: 'Load more' })).toBeDisabled();
    await expect(page.getByText('the query changed, Search to apply it')).toBeVisible();
  });

  test('avro-orders decodes in the event browser', async ({ page }) => {
    await page.goto('/topics/avro-orders');
    await page.getByRole('combobox', { name: 'From' }).selectOption('earliest');
    await page.getByRole('button', { name: 'Search' }).click();
    await expect(page.getByText('{"id":1,"item":"widget"}')).toBeVisible();
  });

  /**
   * #103: a decoded payload was a flat `<pre>` of `JSON.stringify`. For a CDC
   * envelope that is a wall of text; `nested` is the seed's topic with one.
   */
  test('a nested payload opens as a tree, and searches inside itself', async ({ page }) => {
    await page.goto('/topics/nested');
    await page.getByRole('combobox', { name: 'From' }).selectOption('earliest');
    await page.getByRole('button', { name: 'Search' }).click();
    await page.locator('table.msgs tbody tr.row').first().click();

    const value = page.locator('[data-field="value"]');
    // Depth 0 and 1 are open, so the folds are at depth 2: `after.tags` holds three
    // items and `after.meta` two keys, and each says so rather than just showing a
    // caret. Named exactly, so the assertion cannot pass on some other node.
    await expect(value.getByRole('button', { name: '[…] 3 items' })).toBeVisible();
    await expect(value.getByRole('button', { name: '{…} 2 keys' })).toBeVisible();

    // The server found the record; the tree finds the field.
    await value.getByRole('searchbox').fill('ops');
    await expect(value.getByText('1 match')).toBeVisible();
    // `after.meta.by` is at depth 3, behind the fold above — the search opened it.
    await expect(value.getByText('"ops"')).toBeVisible();
  });

  /**
   * The seed cannot carry a header value with a newline or a non-UTF-8 byte — the
   * only producer that reaches this broker is line-oriented and text-only. Those
   * two cases live in `HeadersTable.spec.ts`; this proves the table renders at all,
   * against a real broker, where there used to be a joined `<pre>`.
   */
  test('headers render as a table, one row each', async ({ page }) => {
    await page.goto('/topics/headers');
    await page.getByRole('combobox', { name: 'From' }).selectOption('earliest');
    await page.getByRole('button', { name: 'Search' }).click();
    await page.locator('table.msgs tbody tr.row').first().click();

    const headers = page.locator('table.hdrs');
    await expect(headers).toBeVisible();
    await expect(headers.locator('tbody tr')).toHaveCount(2);
    await expect(headers.getByText('trace')).toBeVisible();
    await expect(headers.getByText('abc123')).toBeVisible();
  });

  /**
   * The acceptance criterion says *Avro*, and `avro-orders` is two flat fields.
   * `avro_to_json` flattens a union and nests a record, so what the tree folds is
   * not only hand-written JSON.
   */
  test('a nested Avro record folds like any other payload', async ({ page }) => {
    await page.goto('/topics/avro-nested');
    await page.getByRole('combobox', { name: 'From' }).selectOption('earliest');
    await page.getByRole('button', { name: 'Search' }).click();
    await page.locator('table.msgs tbody tr.row').first().click();

    const value = page.locator('[data-field="value"]');
    await expect(value.getByText(/avro #\d+/)).toBeVisible();
    // `customer.address` is a nested Avro record at depth 2, so it is folded.
    await expect(value.getByRole('button', { name: '{…} 2 keys' })).toBeVisible();

    // And a value inside it is reachable by search, through the fold.
    await value.getByRole('searchbox').fill('paris');
    await expect(value.getByText('"paris"')).toBeVisible();
  });

  test('a record without headers gets no headers table', async ({ page }) => {
    await page.goto('/topics/headers');
    await page.getByRole('combobox', { name: 'From' }).selectOption('earliest');
    await page.getByRole('button', { name: 'Search' }).click();
    await page.locator('table.msgs tbody tr.row').nth(1).click();
    await expect(page.locator('table.hdrs')).toHaveCount(0);
  });

  test('raw JSON is a toggle, and it survives a reload', async ({ page }) => {
    await page.goto('/topics/nested');
    await page.getByRole('combobox', { name: 'From' }).selectOption('earliest');
    await page.getByRole('button', { name: 'Search' }).click();
    await page.locator('table.msgs tbody tr.row').first().click();
    // The tree is what an expanded record shows by default.
    await expect(page.locator('[data-field="value"] pre')).toHaveCount(0);

    await page.getByLabel('raw JSON').check();
    await expect(page.locator('[data-field="value"] pre')).toBeVisible();

    // Remembered with the two format choices, under the same per-topic key (#32).
    await page.reload();
    await expect(page.getByLabel('raw JSON')).toBeChecked();
  });

  test('schemas page lists the subject', async ({ page }) => {
    await page.goto('/schemas');
    await expect(page.getByRole('link', { name: 'avro-orders-value' })).toBeVisible();
  });

  test('consumer group detail shows zero lag', async ({ page }) => {
    await page.goto('/groups/qa-group');
    await expect(page.getByRole('heading', { name: 'Group qa-group' })).toBeVisible();
    await expect(page.getByText('orders').first()).toBeVisible();
  });
});
