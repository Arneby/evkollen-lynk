const BASE_URL = 'https://www.wayke.se/api/search/vehicles';
const PAGE_SIZE = 24;

const HEADERS = {
  'accept': 'application/json',
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
};

async function fetchPage(w, offset = 0) {
  const params = new URLSearchParams({
    manufacturer: w.manufacturer,
    modelSeries: w.model_series,
    hits: PAGE_SIZE,
    offset,
  });
  if (w.fuel_type)       params.set('fuelTypes', w.fuel_type);
  if (w.mileage_max)    params.set('mileage.max', w.mileage_max);
  if (w._year_from)     params.set('modelYear.min', w._year_from);
  if (w._year_to)       params.set('modelYear.max', w._year_to);

  const res = await fetch(`${BASE_URL}?${params}`, { headers: HEADERS });
  if (!res.ok) throw new Error(`wayke API ${res.status}`);
  return res.json();
}

function normalizeTitle(s) {
  return s.replace(/R[\s-]?Dynamic/gi, 'R-Dynamic');
}

export async function scrape(model, w, rates) {
  const allListings = [];
  let offset = 0;

  while (true) {
    const data = await fetchPage(w, offset);
    const docs = data.documentList?.documents ?? [];
    const total = data.documentList?.numberOfHits ?? 0;

    if (offset === 0) console.log(`  [wayke] Total: ${total}`);
    if (docs.length === 0) break;

    const today = new Date().toISOString().split('T')[0];
    const filter = w.filter || {};
    const include = filter.include || [];
    const exclude = filter.exclude || [];

    for (const doc of docs) {
      const normText = normalizeTitle(`${doc.title} ${doc.shortDescription ?? ''}`);
      const searchText = normText.toLowerCase();

      if (include.length && !include.some(k => searchText.includes(k.toLowerCase()))) continue;
      if (exclude.some(k => searchText.includes(k.toLowerCase()))) continue;

      const versionKeywords = w.version_keywords || [];
      const version = versionKeywords.find(k => searchText.includes(k.toLowerCase())) ?? null;

      const versionFilter = w.version_filter || [];
      if (versionFilter.length && (!version || !versionFilter.some(f => version.toLowerCase().startsWith(f.toLowerCase())))) continue;

      if (!doc.price) continue;

      const imageUrl = doc.featuredImage?.files?.[0]?.formats?.find(f => f.format === '770x')?.url ?? null;
      const url = `https://www.wayke.se/fordon/${doc._id}`;

      allListings.push({
        id: `wayke:${doc._id}`,
        model_id: model.id,
        source: 'wayke',
        url,
        title: doc.title,
        version,
        year: doc.modelYear ?? null,
        km: doc.mileage ?? null,
        price: doc.price ?? null,
        price_financed: null,
        price_eur: doc.price ? Math.round(doc.price / (rates?.SEK ?? 11.5)) : null,
        currency: 'SEK',
        image_url: imageUrl,
        province: null,
        dealer_name: doc.branches?.[0]?.name ?? null,
        is_professional: 1,
        first_seen: today,
        last_seen: today,
      });
    }

    offset += docs.length;
    if (offset >= total) break;
    await new Promise(r => setTimeout(r, 1000));
  }

  console.log(`  [wayke] Matched: ${allListings.length}`);
  return allListings;
}
