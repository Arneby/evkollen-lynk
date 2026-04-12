import { randomUUID } from 'crypto';

const PAGE_SIZE = 30;

const BASE_HEADERS = {
  'accept': 'application/json',
  'accept-language': 'es-ES',
  'content-type': 'application/json',
  'origin': 'https://www.coches.net',
  'referer': 'https://www.coches.net/',
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'x-adevinta-channel': 'web-desktop',
  'x-adevinta-page-url': 'https://www.coches.net/',
  'x-adevinta-referer': 'https://www.coches.net/',
  'x-schibsted-tenant': 'coches',
};

function buildRequestBody(cn, page = 1) {
  return {
    pagination: { page, size: PAGE_SIZE },
    sort: { order: 'desc', term: 'relevance' },
    filters: {
      price: { from: null, to: null },
      bodyTypeIds: [],
      categories: { category1Ids: [2500] },
      drivenWheelsIds: [],
      entry: null,
      environmentalLabels: [],
      equipments: [],
      fuelTypeIds: [],
      hasPhoto: false,
      hasWarranty: false,
      hp: { from: null, to: null },
      isCertified: false,
      km: { from: null, to: null },
      luggageCapacity: { from: null, to: null },
      maxTerms: null,
      onlyPeninsula: false,
      offerTypeIds: [2],   // 2 = KM0
      provinceIds: [],
      searchText: '',
      sellerTypeId: 0,
      transmissionTypeId: null,
      vehicles: [{ makeId: cn.make_id, modelId: cn.model_id, model: '', version: '' }],
      year: { from: null, to: cn.year_to ?? null },
    },
  };
}

async function fetchPage(cn, page) {
  const res = await fetch('https://web.gw.coches.net/search/listing', {
    method: 'POST',
    headers: { ...BASE_HEADERS, 'x-adevinta-session-id': randomUUID() },
    body: JSON.stringify(buildRequestBody(cn, page)),
  });
  if (!res.ok) throw new Error(`coches.net API ${res.status}`);
  return res.json();
}

function normalizeTitle(s) {
  return s.replace(/R[\s-]?Dynamic/gi, 'R-Dynamic');
}

export async function scrape(model, cn, rates) {
  const allListings = [];
  let page = 1;

  while (true) {
    const data = await fetchPage(cn, page);
    const items = data.items || [];
    const totalItems = data.pagination?.totalItems ?? items.length;

    if (page === 1) console.log(`  [coches.net] Total: ${totalItems}`);

    const today = new Date().toISOString().split('T')[0];
    const filter = cn.filter || {};
    const include = filter.include || [];
    const exclude = filter.exclude || [];

    for (const item of items) {
      const normTitle = normalizeTitle(item.title);
      const titleLc = normTitle.toLowerCase();
      if (include.length && !include.some(k => titleLc.includes(k.toLowerCase()))) continue;
      if (exclude.some(k => titleLc.includes(k.toLowerCase()))) continue;

      const versionKeywords = cn.version_keywords || [];
      const version = versionKeywords.find(k => titleLc.includes(k.toLowerCase())) ?? null;

      const versionFilter = cn.version_filter || [];
      if (versionFilter.length && (!version || !versionFilter.some(f => version.toLowerCase().startsWith(f.toLowerCase())))) continue;

      if (model.year && item.year !== model.year) continue;
      if (!item.price?.amount) continue;

      const url = item.url.startsWith('http') ? item.url : `https://www.coches.net${item.url}`;
      const imageUrl = item.resources?.find(r => r.type === 'IMAGE')?.url ?? null;

      allListings.push({
        id: `coches_net:${item.id}`,
        model_id: model.id,
        source: 'coches_net',
        url,
        title: item.title,
        version,
        year: item.year ?? null,
        km: item.km ?? null,
        price: item.price?.amount ?? null,
        price_financed: item.price?.financedAmount ?? null,
        price_eur: item.price?.amount ?? null,
        currency: 'EUR',
        image_url: imageUrl,
        province: item.mainProvince ?? null,
        dealer_name: item.seller?.name ?? null,
        is_professional: item.seller?.isProfessional ? 1 : 0,
        first_seen: today,
        last_seen: today,
      });
    }

    const fetched = (page - 1) * PAGE_SIZE + items.length;
    if (fetched >= totalItems || items.length === 0) break;
    page++;
    await new Promise(r => setTimeout(r, 2000));
  }

  console.log(`  [coches.net] Matched: ${allListings.length}`);
  return allListings;
}
