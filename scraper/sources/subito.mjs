const BASE_URL = 'https://www.subito.it/annunci-italia/vendita/auto';

const HEADERS = {
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'accept-language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
};

function buildUrl(s, yearFrom, yearTo, start) {
  let base;
  if (s.make_slug && s.model_slug) {
    base = `${BASE_URL}/${s.make_slug}/${s.model_slug}/`;
  } else if (s.make_slug) {
    base = `${BASE_URL}/${s.make_slug}/`;
  } else {
    base = `${BASE_URL}/`;
  }

  const params = new URLSearchParams();
  if (yearFrom) params.set('ys', yearFrom);
  if (yearTo)   params.set('ye', yearTo);
  if (s.query)  params.set('q', s.query);
  if (start)    params.set('start', start);
  return `${base}?${params}`;
}

function normalizeTitle(s) {
  return s.replace(/R[\s-]?Dynamic/gi, 'R-Dynamic');
}

function parseNextData(html) {
  const m = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
  if (!m) return null;
  try { return JSON.parse(m[1]); } catch { return null; }
}

function getFeature(item, key) {
  return item.features?.[key]?.values?.[0]?.key ?? null;
}

async function fetchPage(s, yearFrom, yearTo, start) {
  const url = buildUrl(s, yearFrom, yearTo, start);
  const res = await fetch(url, { headers: HEADERS });
  if (!res.ok) throw new Error(`subito.it HTTP ${res.status} for ${url}`);
  return res.text();
}

export async function scrape(model, s, rates) {
  const allListings = [];
  const year = model.year ?? null;
  let start = 0;

  while (true) {
    const html = await fetchPage(s, year, year, start || null);
    const data = parseNextData(html);
    if (!data) throw new Error('subito.it: no __NEXT_DATA__ found');

    const state = data.props?.pageProps?.initialState;
    const list  = state?.items?.list ?? [];
    const total = state?.items?.total ?? 0;

    if (start === 0) console.log(`  [subito.it] Total: ${total}`);
    if (list.length === 0) break;

    const today = new Date().toISOString().split('T')[0];
    const filter = s.filter || {};
    const include = filter.include || [];
    const exclude = filter.exclude || [];

    for (const decorated of list) {
      const item = decorated.item;
      if (!item) continue;

      const rawTitle = item.subject ?? '';
      const normTitle = normalizeTitle(rawTitle);
      const titleLc = normTitle.toLowerCase();

      if (include.length && !include.some(k => titleLc.includes(k.toLowerCase()))) continue;
      if (exclude.some(k => titleLc.includes(k.toLowerCase()))) continue;

      const priceEur = getFeature(item, '/price');
      const itemYear = getFeature(item, '/year');
      const km       = getFeature(item, '/mileage_scalar');

      // Filter leasing monthly prices (anything under 5000 EUR is a monthly rate)
      if (!priceEur || priceEur < 5000) continue;

      // Year filter
      if (year && itemYear !== null && parseInt(itemYear) !== year) continue;

      const versionKeywords = s.version_keywords || [];
      const version = versionKeywords.find(k => titleLc.includes(k.toLowerCase())) ?? null;

      const versionFilter = s.version_filter || [];
      if (versionFilter.length && (!version || !versionFilter.some(f => version.toLowerCase().startsWith(f.toLowerCase())))) continue;

      const url      = item.urls?.default ?? null;
      const dealer   = item.advertiser?.shopName ?? item.advertiser?.name ?? null;
      const imageUrl = item.images?.[0]
        ? `${item.images[0].cdnBaseUrl}/400x300.jpg`
        : null;

      // Extract ad ID from urn (format: "id:ad:uuid:list:numericId")
      const urnParts = (item.urn ?? '').split(':');
      const id = urnParts[urnParts.length - 1] || item.urn;

      allListings.push({
        id:             `subito:${id}`,
        model_id:       model.id,
        source:         'subito',
        url,
        title:          rawTitle,
        version,
        year:           itemYear ? parseInt(itemYear) : null,
        km:             km ? parseInt(km) : null,
        price:          parseInt(priceEur),
        price_financed: null,
        price_eur:      parseInt(priceEur),
        currency:       'EUR',
        image_url:      imageUrl,
        province:       null,
        dealer_name:    dealer,
        is_professional: item.advertiser?.type === 'c' ? 1 : 0,
        first_seen:     today,
        last_seen:      today,
      });
    }

    start += list.length;
    if (start >= total) break;
    await new Promise(r => setTimeout(r, 1500));
  }

  console.log(`  [subito.it] Matched: ${allListings.length}`);
  return allListings;
}
