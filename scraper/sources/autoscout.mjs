const BASE_URL = 'https://www.autoscout24.de/lst';

const HEADERS = {
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
};

const MAX_PAGES = 5;

function buildUrl(s, yearFrom, yearTo, page) {
  const path = s.model_slug
    ? `${BASE_URL}/${s.make_slug}/${s.model_slug}`
    : `${BASE_URL}/${s.make_slug}`;

  const params = new URLSearchParams();
  params.set('atype', 'C');
  params.set('cy', s.country ?? 'D');
  if (s.fuel_type) params.set('fuelc', s.fuel_type);
  if (yearFrom) params.set('fregfrom', yearFrom);
  if (yearTo)   params.set('fregto',   yearTo);
  if (s.query) params.set('q', s.query);
  if (page > 1) params.set('page', page);
  return `${path}?${params}`;
}

function parseNextData(html) {
  const m = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
  if (!m) return null;
  try { return JSON.parse(m[1]); } catch { return null; }
}

function parseYear(firstReg) {
  // Format: "MM-YYYY"
  if (!firstReg) return null;
  const parts = firstReg.split('-');
  return parts.length === 2 ? parseInt(parts[1]) : null;
}

export async function scrape(model, s, rates) {
  const allListings = [];
  const year     = model.year ?? null;
  const yearFrom = s._year_from ?? year;
  const yearTo   = s._year_to   ?? year;
  let page = 1;
  let totalPages = 1;

  while (page <= Math.min(totalPages, MAX_PAGES)) {
    const url = buildUrl(s, yearFrom, yearTo, page);
    const res = await fetch(url, { headers: HEADERS });
    if (!res.ok) throw new Error(`autoscout24.de HTTP ${res.status} for ${url}`);
    const html = await res.text();

    const data = parseNextData(html);
    if (!data) throw new Error('autoscout24.de: no __NEXT_DATA__ found');

    const pp = data.props?.pageProps ?? {};
    const listings = pp.listings ?? [];
    totalPages = pp.numberOfPages ?? 1;

    if (page === 1) console.log(`  [autoscout24] Total: ${pp.numberOfResults ?? 0}`);
    if (listings.length === 0) break;

    const today = new Date().toISOString().split('T')[0];
    const filter = s.filter || {};
    const include = filter.include || [];
    const exclude = filter.exclude || [];

    for (const listing of listings) {
      const vehicle = listing.vehicle ?? {};
      const tracking = listing.tracking ?? {};

      const rawTitle = vehicle.modelVersionInput ?? '';
      const titleLc = rawTitle.toLowerCase();

      if (include.length && !include.some(k => titleLc.includes(k.toLowerCase()))) continue;
      if (exclude.some(k => titleLc.includes(k.toLowerCase()))) continue;

      const priceEur = tracking.price ? parseInt(tracking.price) : null;
      const km = tracking.mileage ? parseInt(tracking.mileage) : null;
      const itemYear = parseYear(tracking.firstRegistration);

      if (!priceEur || priceEur < 5000) continue;
      if (itemYear !== null && yearFrom && itemYear < yearFrom) continue;
      if (itemYear !== null && yearTo   && itemYear > yearTo)   continue;

      const versionKeywords = s.version_keywords || [];
      const version = versionKeywords.find(k => titleLc.includes(k.toLowerCase())) ?? null;

      const versionFilter = s.version_filter || [];
      if (versionFilter.length && (!version || !versionFilter.some(f => version.toLowerCase().startsWith(f.toLowerCase())))) continue;

      allListings.push({
        id:              `autoscout:${listing.id}`,
        model_id:        model.id,
        source:          'autoscout',
        url:             listing.url ? `https://www.autoscout24.de${listing.url}` : null,
        title:           rawTitle,
        version,
        year:            itemYear,
        km,
        price:           priceEur,
        price_financed:  null,
        price_eur:       priceEur,
        currency:        'EUR',
        image_url:       listing.images?.[0] ?? null,
        province:        listing.location?.city ?? null,
        dealer_name:     listing.seller?.companyName ?? listing.seller?.contactName ?? null,
        is_professional: listing.seller?.type === 'Dealer' ? 1 : 0,
        first_seen:      today,
        last_seen:       today,
      });
    }

    page++;
    if (page <= Math.min(totalPages, MAX_PAGES)) await new Promise(r => setTimeout(r, 1500));
  }

  console.log(`  [autoscout24] Matched: ${allListings.length}`);
  return allListings;
}
