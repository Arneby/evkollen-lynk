import { scrape as scrapeCoches }    from './sources/coches_net.js';
import { scrape as scrapeAutoscout } from './sources/autoscout.js';
import { scrape as scrapeBytbil }    from './sources/bytbil.js';
import { scrape as scrapeSubito }    from './sources/subito.js';

const SCRAPERS = {
  coches_net: scrapeCoches,
  autoscout:  scrapeAutoscout,
  bytbil:     scrapeBytbil,
  subito:     scrapeSubito,
};

const MODELS = [
  {
    id: 'lynk-01-phev-more', make: 'Lynk & Co', model: '01', variant: 'More', powertrain: 'phev', year: 2025,
    sources: {
      coches_net: { make_id: 1404, model_id: 1336, filter: { include: ['More'] } },
      bytbil:     { make: 'Lynk & Co', model: '01', filter: { include: ['More'] } },
      subito:     { query: 'Lynk 01 PHEV More', filter: { include: ['01', 'More'] } },
      autoscout:  { make_slug: 'lynk-%26-co', model_slug: '01', fuel_type: 'H', filter: { include: ['More'] } },
    }
  },
  {
    id: 'lynk-02-phev-more', make: 'Lynk & Co', model: '02', variant: 'More', powertrain: 'phev', year: 2025,
    sources: {
      coches_net: { make_id: 1404, model_id: 1601, filter: { include: ['More'] } },
      bytbil:     { make: 'Lynk & Co', model: '02', filter: { include: ['More'] } },
      subito:     { query: 'Lynk 02 PHEV More', filter: { include: ['02', 'More'] } },
      autoscout:  { make_slug: 'lynk-%26-co', model_slug: '02', fuel_type: 'H', filter: { include: ['More'] } },
    }
  },
  {
    id: 'lynk-08-phev-more', make: 'Lynk & Co', model: '08', variant: 'More', powertrain: 'phev', year: 2025,
    sources: {
      coches_net: { make_id: 1404, model_id: 1625, filter: { include: ['More'] } },
      bytbil:     { make: 'Lynk & Co', model: '08', filter: { include: ['More'] } },
      subito:     { query: 'Lynk 08 PHEV', filter: { include: ['08'] } },
      autoscout:  { make_slug: 'lynk-%26-co', model_slug: '08', fuel_type: 'H', filter: { include: ['More'] } },
    }
  },
];

const DELAY_MS = [5000, 10000];

async function fetchEurRates() {
  const res = await fetch('https://api.frankfurter.app/latest?from=EUR&to=SEK');
  if (!res.ok) throw new Error(`Frankfurter API ${res.status}`);
  const data = await res.json();
  return { SEK: data.rates.SEK, EUR: 1 };
}

function deduplicateListings(listings) {
  const SWEDISH = new Set(['wayke', 'bytbil']);
  const kept = [];
  for (const a of listings) {
    if (!SWEDISH.has(a.source)) { kept.push(a); continue; }
    const isDup = kept.some(b => {
      if (!SWEDISH.has(b.source) || b.source === a.source) return false;
      if (a.year !== b.year) return false;
      if (a.km != null && b.km != null && Math.abs(a.km - b.km) > 500) return false;
      const wA = (a.dealer_name ?? '').split(/\s+/)[0].toLowerCase();
      const wB = (b.dealer_name ?? '').split(/\s+/)[0].toLowerCase();
      return wA.length > 2 && wA === wB;
    });
    if (!isDup) kept.push(a);
  }
  return kept;
}

async function ingestListings(listings, env) {
  const now   = new Date().toISOString();
  const today = now.split('T')[0];
  let inserted = 0, updated = 0;

  for (const l of listings) {
    const existing = await env.DB.prepare(
      'SELECT id FROM listings WHERE source = ? AND url = ?'
    ).bind(l.source, l.url).first();

    if (!existing) {
      await env.DB.prepare(
        `INSERT OR IGNORE INTO listings
         (id, model_id, source, url, title, version, year, km, price, price_financed, price_eur, currency, image_url, province, dealer_name, is_professional, first_seen, last_seen)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).bind(
        l.id, l.model_id, l.source, l.url, l.title ?? null, l.version ?? null,
        l.year ?? null, l.km ?? null, l.price ?? null, l.price_financed ?? null,
        l.price_eur ?? null, l.currency ?? 'EUR', l.image_url ?? null,
        l.province ?? null, l.dealer_name ?? null, l.is_professional ?? 1,
        today, today
      ).run();
      inserted++;
    } else {
      await env.DB.prepare(
        `UPDATE listings SET last_seen=?, km=?, price=?, price_financed=?, price_eur=?, image_url=?, title=?, version=? WHERE id=?`
      ).bind(today, l.km ?? null, l.price ?? null, l.price_financed ?? null,
        l.price_eur ?? null, l.image_url ?? null, l.title ?? null, l.version ?? null, existing.id
      ).run();
      updated++;
    }

    if (l.price) {
      const listingId = existing ? existing.id : l.id;
      await env.DB.prepare(
        'INSERT INTO price_snapshots (listing_id, price, price_eur, km, currency, scraped_at) VALUES (?, ?, ?, ?, ?, ?)'
      ).bind(listingId, l.price, l.price_eur ?? null, l.km ?? null, l.currency ?? 'EUR', now).run();
    }
  }
  return { inserted, updated };
}

export async function runScraper(env) {
  console.log(`[scraper] start — ${new Date().toISOString()}`);
  const rates = await fetchEurRates();
  console.log(`[scraper] EUR/SEK: ${rates.SEK}`);

  let totalInserted = 0, totalUpdated = 0;

  for (const model of MODELS) {
    console.log(`[scraper] → ${model.make} ${model.model} ${model.variant}`);
    const allListings = [];

    for (const [sourceName, sourceConfig] of Object.entries(model.sources)) {
      const scrape = SCRAPERS[sourceName];
      if (!scrape) continue;
      try {
        const cfg = { ...sourceConfig, _year_from: model.year, _year_to: model.year };
        const listings = await scrape(model, cfg, rates);
        allListings.push(...listings);
      } catch (err) {
        console.error(`[scraper] [${sourceName}] error: ${err.message}`);
      }
    }

    const deduped = deduplicateListings(allListings);
    if (deduped.length > 0) {
      const { inserted, updated } = await ingestListings(deduped, env);
      totalInserted += inserted;
      totalUpdated  += updated;
      console.log(`[scraper] ${model.id}: ${inserted} inserted, ${updated} updated`);
    } else {
      console.log(`[scraper] ${model.id}: inga träffar`);
    }

    // Fördröjning mellan modeller
    const delay = Math.floor(Math.random() * (DELAY_MS[1] - DELAY_MS[0]) + DELAY_MS[0]);
    await new Promise(r => setTimeout(r, delay));
  }

  console.log(`[scraper] klart — totalt ${totalInserted} inserted, ${totalUpdated} updated`);
  return { inserted: totalInserted, updated: totalUpdated };
}
