const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-Secret',
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    if (url.pathname === '/health') {
      return Response.json({ ok: true }, { headers: CORS });
    }

    if (url.pathname === '/ingest' && request.method === 'POST') {
      if (request.headers.get('X-Secret') !== env.WORKER_SECRET) {
        return new Response('Unauthorized', { status: 401 });
      }
      try {
        return await handleIngest(request, env);
      } catch (e) {
        return Response.json({ error: e.message, stack: e.stack }, { status: 500 });
      }
    }

    if (url.pathname === '/listings' && request.method === 'GET') {
      return handleListings(request, env);
    }

    if (url.pathname === '/listing' && request.method === 'GET') {
      const id = url.searchParams.get('id');
      if (!id) return Response.json({ error: 'id required' }, { status: 400, headers: CORS });
      const row = await env.DB.prepare('SELECT * FROM listings WHERE id = ?').bind(id).first();
      if (!row) return Response.json({ error: 'not found' }, { status: 404, headers: CORS });
      return Response.json({ listing: row }, { headers: CORS });
    }

    if (url.pathname === '/snapshots' && request.method === 'GET') {
      return handleSnapshots(request, env);
    }

    if (url.pathname === '/price-history' && request.method === 'GET') {
      return handlePriceHistory(request, env);
    }

    if (url.pathname === '/models' && request.method === 'GET') {
      const { results } = await env.DB.prepare(
        'SELECT id, make, model, powertrain, label FROM models ORDER BY rowid ASC'
      ).all();
      return Response.json({ models: results }, { headers: CORS });
    }

    return new Response('Not found', { status: 404 });
  },
};

async function handleIngest(request, env) {
  let body;
  try {
    body = await request.json();
  } catch (e) {
    return Response.json({ error: 'Invalid JSON' }, { status: 400 });
  }

  const { listings, models } = body;
  if (!Array.isArray(listings)) {
    return Response.json({ error: 'listings must be an array' }, { status: 400 });
  }

  if (Array.isArray(models)) {
    for (const m of models) {
      await env.DB.prepare(
        'INSERT OR REPLACE INTO models (id, make, model, powertrain, label) VALUES (?, ?, ?, ?, ?)'
      ).bind(m.id, m.make, m.model, m.powertrain, m.label ?? null).run();
    }
  }

  let inserted = 0;
  let updated = 0;
  const now = new Date().toISOString();
  const today = now.split('T')[0];

  for (const l of listings) {
    const existing = await env.DB.prepare(
      'SELECT id FROM listings WHERE source = ? AND url = ?'
    ).bind(l.source, l.url).first();

    if (!existing) {
      await env.DB.prepare(
        'INSERT OR IGNORE INTO listings (id, model_id, source, url, title, version, year, km, price, price_financed, price_eur, currency, image_url, province, dealer_name, is_professional, first_seen, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
      ).bind(
        l.id, l.model_id, l.source, l.url, l.title ?? null, l.version ?? null,
        l.year ?? null, l.km ?? null, l.price ?? null, l.price_financed ?? null, l.price_eur ?? null, l.currency ?? 'EUR', l.image_url ?? null,
        l.province ?? null, l.dealer_name ?? null,
        l.is_professional ?? 1, today, today
      ).run();
      inserted++;
    } else {
      await env.DB.prepare(
        'UPDATE listings SET last_seen = ?, km = ?, price = ?, price_financed = ?, price_eur = ?, image_url = ?, title = ?, version = ? WHERE id = ?'
      ).bind(today, l.km ?? null, l.price ?? null, l.price_financed ?? null, l.price_eur ?? null, l.image_url ?? null, l.title ?? null, l.version ?? null, existing.id).run();
      updated++;
    }

    if (l.price) {
      const listingId = existing ? existing.id : l.id;
      await env.DB.prepare(
        'INSERT INTO price_snapshots (listing_id, price, price_eur, km, currency, scraped_at) VALUES (?, ?, ?, ?, ?, ?)'
      ).bind(listingId, l.price, l.price_eur ?? null, l.km ?? null, l.currency ?? 'EUR', now).run();
    }
  }

  return Response.json({ inserted, updated }, { headers: CORS });
}

async function handleListings(request, env) {
  const url = new URL(request.url);
  const modelId = url.searchParams.get('model_id');

  const query = modelId
    ? 'SELECT * FROM listings WHERE model_id = ? ORDER BY price ASC'
    : 'SELECT * FROM listings ORDER BY model_id, price ASC';

  const { results } = modelId
    ? await env.DB.prepare(query).bind(modelId).all()
    : await env.DB.prepare(query).all();

  return Response.json({ listings: results }, { headers: CORS });
}

async function handleSnapshots(request, env) {
  const url = new URL(request.url);
  const listingId = url.searchParams.get('listing_id');
  if (!listingId) {
    return Response.json({ error: 'listing_id required' }, { status: 400, headers: CORS });
  }

  const { results } = await env.DB.prepare(
    'SELECT * FROM price_snapshots WHERE listing_id = ? ORDER BY scraped_at ASC'
  ).bind(listingId).all();

  return Response.json({ snapshots: results }, { headers: CORS });
}

async function handlePriceHistory(request, env) {
  const url = new URL(request.url);
  const modelId = url.searchParams.get('model_id');
  if (!modelId) {
    return Response.json({ error: 'model_id required' }, { status: 400, headers: CORS });
  }

  const { results } = await env.DB.prepare(`
    SELECT period, granularity, sort_key, source, avg_price_eur, count FROM (
      SELECT
        strftime('%Y-v%W', ps.scraped_at) AS period,
        'week'                            AS granularity,
        MIN(date(ps.scraped_at))          AS sort_key,
        l.source,
        ROUND(AVG(ps.price_eur))          AS avg_price_eur,
        COUNT(*)                          AS count
      FROM price_snapshots ps
      JOIN listings l ON l.id = ps.listing_id
      WHERE l.model_id = ?
        AND strftime('%Y-%W', ps.scraped_at) != strftime('%Y-%W', date('now'))
      GROUP BY strftime('%Y-v%W', ps.scraped_at), l.source

      UNION ALL

      SELECT
        date(ps.scraped_at)  AS period,
        'day'                AS granularity,
        date(ps.scraped_at)  AS sort_key,
        l.source,
        ROUND(AVG(ps.price_eur)) AS avg_price_eur,
        COUNT(*)             AS count
      FROM price_snapshots ps
      JOIN listings l ON l.id = ps.listing_id
      WHERE l.model_id = ?
        AND strftime('%Y-%W', ps.scraped_at) = strftime('%Y-%W', date('now'))
      GROUP BY date(ps.scraped_at), l.source
    )
    ORDER BY sort_key ASC, period ASC
  `).bind(modelId, modelId).all();

  return Response.json({ history: results }, { headers: CORS });
}
