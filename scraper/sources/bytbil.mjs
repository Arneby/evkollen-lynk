const BASE_URL = 'https://www.bytbil.com';
const PAGE_SIZE = 24;

const HEADERS = {
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'accept-language': 'sv-SE,sv;q=0.9',
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
};

function buildUrl(b, yearFrom, yearTo, page) {
  const params = new URLSearchParams({
    VehicleType: 'bil',
    Makes: b.make,
    Models: b.model,
    'ModelYearRange.From': yearFrom,
    'ModelYearRange.To': yearTo,
    'SortParams.SortField': 'publishedDate',
    'SortParams.IsAscending': 'False',
    Page: page,
  });
  if (b.fuel_type) params.set('Fuels', b.fuel_type);
  if (b.mileage_max != null) params.set('MilageRange.To', b.mileage_max);
  return `${BASE_URL}/bil?${params}`;
}

function normalizeTitle(s) {
  return s.replace(/R[\s-]?Dynamic/gi, 'R-Dynamic');
}

// Extract impressions array from GTM dataLayer — gives id + price reliably
function extractImpressions(html) {
  const m = html.match(/'impressions':\s*(\[[\s\S]*?\])\s*\}/);
  if (!m) return [];
  try { return JSON.parse(m[1]); } catch { return []; }
}

// Parse listing cards from HTML — gives url, year, km, location per car
function extractCards(html) {
  const cards = [];
  const cardRe = /<div[^>]+data-model-id="(\d+)"[^>]*>([\s\S]*?)(?=<div[^>]+data-model-id="|<\/ul>)/g;
  let m;
  while ((m = cardRe.exec(html)) !== null) {
    const id = m[1];
    const chunk = m[2];

    // URL from car-list-header anchor
    const urlM = chunk.match(/href="(\/[^"]+)"/);
    const url = urlM ? BASE_URL + urlM[1] : null;

    // Year: first 4-digit number in uk-text-truncate paragraph
    const yearM = chunk.match(/class="uk-text-truncate"[^>]*>\s*(\d{4})/);
    const year = yearM ? parseInt(yearM[1]) : null;
    // Km: digits before "mil" (strip HTML entities and whitespace)
    const kmM = chunk.match(/class="uk-text-truncate"[\s\S]*?([\d][^<]*?)\s*mil\s*</i);
    const km = kmM ? Math.round(parseInt(kmM[1].replace(/&[^;]+;/g, '').replace(/\D/g, '')) * 10) : null;

    // Image (may be lazy via data-src)
    const imgM = chunk.match(/(?:src|data-src)="(https:\/\/[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"/i);
    const imageUrl = imgM ? imgM[1] : null;

    if (url) cards.push({ id, url, year, km, imageUrl });
  }
  return cards;
}

async function fetchPage(b, yearFrom, yearTo, page) {
  const url = buildUrl(b, yearFrom, yearTo, page);
  const res = await fetch(url, { headers: HEADERS });
  if (!res.ok) throw new Error(`bytbil HTTP ${res.status}`);
  return res.text();
}

export async function scrape(model, b, rates) {
  const allListings = [];
  const yearFrom = b._year_from ?? null;
  const yearTo   = b._year_to   ?? null;
  let page = 1;

  while (true) {
    const html = await fetchPage(b, yearFrom, yearTo, page);
    const impressions = extractImpressions(html);
    const cards = extractCards(html);

    if (page === 1) {
      const totalM = html.match(/(\d+)\s*(?:träff|bil(?:ar)?)\s*(?:hittad|funnen)/i);
      const total = totalM ? totalM[1] : `${impressions.length}+`;
      console.log(`  [bytbil] Total: ${total}`);
    }

    if (impressions.length === 0) break;

    const today = new Date().toISOString().split('T')[0];
    const filter = b.filter || {};
    const include = filter.include || [];
    const exclude = filter.exclude || [];

    // Merge impressions + cards by ID
    const cardById = Object.fromEntries(cards.map(c => [c.id, c]));

    for (const imp of impressions) {
      const card = cardById[imp.id] || {};
      const normTitle = normalizeTitle(imp.name);
      const titleLc = normTitle.toLowerCase();

      if (include.length && !include.some(k => titleLc.includes(k.toLowerCase()))) continue;
      if (exclude.some(k => titleLc.includes(k.toLowerCase()))) continue;

      const cardYear = card.year ?? null;
      // Require year within range — also filters leasing ads (no year in card)
      if (yearFrom && (cardYear === null || cardYear < yearFrom)) continue;
      if (yearTo   && (cardYear === null || cardYear > yearTo))   continue;

      const versionKeywords = b.version_keywords || [];
      const version = versionKeywords.find(k => titleLc.includes(k.toLowerCase())) ?? null;

      const versionFilter = b.version_filter || [];
      if (versionFilter.length && (!version || !versionFilter.some(f => version.toLowerCase().startsWith(f.toLowerCase())))) continue;

      const priceKr = imp.price ? parseInt(imp.price) : null;
      if (!priceKr || priceKr < 50000) continue;   // filter leasing monthly prices
      const priceEur = Math.round(priceKr / (rates?.SEK ?? 11.5));
      const url = card.url ?? `${BASE_URL}/bil/${imp.id}`;

      allListings.push({
        id: `bytbil:${imp.id}`,
        model_id: model.id,
        source: 'bytbil',
        url,
        title: imp.name,
        version,
        year: cardYear,
        km: card.km ?? null,
        price: priceKr,
        price_financed: null,
        price_eur: priceEur,
        currency: 'SEK',
        image_url: card.imageUrl ?? null,
        province: null,
        dealer_name: imp.dimension7 ?? null,
        is_professional: 1,
        first_seen: today,
        last_seen: today,
      });
    }

    if (impressions.length < PAGE_SIZE) break;
    page++;
    await new Promise(r => setTimeout(r, 1500));
  }

  console.log(`  [bytbil] Matched: ${allListings.length}`);
  return allListings;
}
