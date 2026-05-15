/**
 * StreamHub – Cloudflare Worker
 * RTL Play + RTBF Auvio + TF1+
 * Bucketing thématique via Cloudflare AI (gratuit)
 */

export interface Env {
  KV: KVNamespace;
  AI: Ai;
}

const CACHE_TTL = 600;

// ─── Types ────────────────────────────────────────────────────────────────────

export interface NormalizedItem {
  id:          string;
  platform:    'RTLplay' | 'RTBF' | 'TF1+';
  title:       string;
  description: string;
  imageUrl:    string;
  path:        string;
  type:        string;
  labels:      string[];
  _raw:        any;
}

export interface ThematicBucket {
  theme:   string;
  label:   string;
  emoji:   string;
  items:   NormalizedItem[];
  hasMore: boolean;
}

// ─── RTL Play ─────────────────────────────────────────────────────────────────

async function fetchRTL(): Promise<NormalizedItem[]> {
  const res = await fetch(
    'https://lfvp-api.dpgmedia.net/RTL_PLAY/storefronts/accueil?itemsPerSwimlane=30&hideBannerRow=true',
    {
      headers: {
        'User-Agent':            'RTL_PLAY/25.260415 (com.tapptic.rtl.tvi; build:30644; Android 30)',
        'Accept':                'application/json',
        'lfvp-device-segment':   'TV>Android',
        'x-app-version':         '25',
      },
    },
  );
  if (!res.ok) throw new Error(`RTL ${res.status}`);
  const data = await res.json() as any;

  const items: NormalizedItem[] = [];
  for (const row of data?.rows ?? []) {
    if (!row.teasers) continue;
    for (const t of row.teasers) {
      if (!t.title || !t.imageUrl) continue;
      items.push({
        id:          `rtl-${t.detailId ?? t.title}`,
        platform:    'RTLplay',
        title:       t.title,
        description: t.description ?? '',
        // Image landscape 16:9 → affichée en portrait 2:3 via object-fit:cover (cf. VideoCard)
        imageUrl:    t.imageUrl,
        path:        t.detailId ? `/detail/${t.detailId}` : '',
        type:        guessTypeFromLabels(t.labels),
        labels:      (t.labels ?? []).map((l: any) => l.label ?? l),
        _raw:        { ...t, platform: 'RTLplay' },
      });
    }
  }
  return items;
}

// ─── RTBF ─────────────────────────────────────────────────────────────────────

async function fetchRTBF(): Promise<NormalizedItem[]> {
  const headers = { 'User-Agent': 'Mozilla/5.0 Chrome/124', 'Accept': 'application/json' };

  const homeRes = await fetch(
    'https://bff-service.rtbf.be/auvio/v1.23/pages/home?userAgent=Chrome-web-3.0',
    { headers },
  );
  if (!homeRes.ok) throw new Error(`RTBF ${homeRes.status}`);
  const home = await homeRes.json() as any;

  const toFetch = (home.data?.widgets ?? [] as any[])
    .filter((w: any) => ['PROGRAM_LIST', 'MEDIA_LIST'].includes(w.type) && w.contentPath)
    .slice(0, 8);

  const results = await Promise.allSettled(
    toFetch.map((w: any) => fetch(w.contentPath, { headers }).then(r => r.json())),
  );

  const items: NormalizedItem[] = [];
  for (const r of results) {
    if (r.status !== 'fulfilled') continue;
    const list = Array.isArray(r.value?.data) ? r.value.data : [];
    for (const item of list) {
      const illu = item.illustration;
      if (!illu) continue;
      items.push({
        id:          `rtbf-${item.id}`,
        platform:    'RTBF',
        title:       item.title ?? item.name ?? '',
        description: item.description ?? '',
        // RTBF illustration est 2x3 portrait natif ✓
        imageUrl:    illu.m ?? illu.s ?? illu.l ?? illu.xs ?? '',
        path:        item.path ?? '',
        type:        item.type ?? 'SHOW',
        labels:      [],
        _raw:        { ...item, platform: 'RTBF' },
      });
    }
  }
  return items;
}

// ─── TF1+ ─────────────────────────────────────────────────────────────────────

async function fetchTF1(): Promise<NormalizedItem[]> {
  const variables = encodeURIComponent(JSON.stringify({
    context: { persona: 'PERSONA_2', application: 'WEB', device: 'DESKTOP', os: 'WINDOWS' },
    filter:  { channel: '' },
    offset:  0,
    limit:   100,
  }));
  const res = await fetch(
    `https://www.tf1.fr/graphql/web?id=483ce0f&variables=${variables}`,
    { headers: { 'User-Agent': 'Mozilla/5.0 Chrome/124', 'Accept': 'application/json', 'Origin': 'https://www.tf1.fr' } },
  );
  if (!res.ok) throw new Error(`TF1 ${res.status}`);
  const data = await res.json() as any;

  const items: NormalizedItem[] = [];
  for (const p of data?.data?.programs?.items ?? []) {
    const deco = p.decoration;
    if (!deco) continue;
    // Priorité PORTRAIT (2:3) → fallback thumbnail landscape
    const src  = deco.image?.sources?.length ? deco.image.sources : deco.thumbnail?.sources ?? [];
    const best = src.find((s: any) => s.type === 'webp') ?? src[0];
    items.push({
      id:          `tf1-${p.id}`,
      platform:    'TF1+',
      title:       p.name ?? '',
      description: deco.description ?? '',
      imageUrl:    best?.url ?? '',
      path:        `/programme/${p.slug ?? p.id}`,
      type:        tf1Type(p.categories),
      labels:      (p.categories ?? []).map((c: any) => c.label),
      _raw:        { ...p, platform: 'TF1+' },
    });
  }
  return items;
}

// ─── Hero banners ─────────────────────────────────────────────────────────────

async function fetchHeroBanners(): Promise<any[]> {
  try {
    const res = await fetch(
      'https://lfvp-api.dpgmedia.net/RTL_PLAY/storefronts/accueil?itemsPerSwimlane=5&hideBannerRow=false',
      {
        headers: {
          'User-Agent':          'RTL_PLAY/25.260415 (com.tapptic.rtl.tvi; build:30644; Android 30)',
          'Accept':              'application/json',
          'lfvp-device-segment': 'TV>Android',
          'x-app-version':       '25',
        },
      },
    );
    if (!res.ok) return [];
    const data  = await res.json() as any;
    const top   = (data?.rows ?? []).find((r: any) => r.rowType === 'TOP_BANNER');
    if (!top?.teaser) return [];
    const t = top.teaser;
    return [{
      platform:      'RTLplay',
      title:         t.title ?? '',
      description:   t.byline ?? '',
      largeImageUrl: t.largeImageUrl ?? t.mediumImageUrl ?? '',
      detailId:      t.target?.id ?? null,
    }];
  } catch {
    return [];
  }
}

// ─── Cloudflare AI : bucketing ────────────────────────────────────────────────

const THEMES = [
  { key: 'series',   label: 'Séries',        emoji: '📺' },
  { key: 'films',    label: 'Films',          emoji: '🎬' },
  { key: 'reality',  label: 'Téléréalité',    emoji: '🎭' },
  { key: 'news',     label: 'Info & Actu',    emoji: '📰' },
  { key: 'kids',     label: 'Enfants',        emoji: '🧸' },
  { key: 'docs',     label: 'Documentaires',  emoji: '🔍' },
  { key: 'comedy',   label: 'Comédie',        emoji: '😄' },
  { key: 'sport',    label: 'Sport',          emoji: '⚽' },
  { key: 'trending', label: 'En ce moment',   emoji: '🔥' },
] as const;

async function aiClassify(items: NormalizedItem[], ai: Ai): Promise<ThematicBucket[]> {
  const BATCH = 50;
  const assignments: Record<number, string> = {};

  for (let start = 0; start < items.length; start += BATCH) {
    const batch   = items.slice(start, start + BATCH);
    const payload = batch.map((it, i) => ({ i: start + i, t: it.title, d: it.description.slice(0, 40) }));

    const prompt = `Classifie chaque contenu dans UN thème : series, films, reality, news, kids, docs, comedy, sport, trending.
Réponds UNIQUEMENT en JSON valide sans markdown : {"assignments":{"0":"series","1":"films",...}}
Contenus : ${JSON.stringify(payload)}`;

    try {
      const r    = await (ai as any).run('@cf/meta/llama-3.1-8b-instruct', { messages: [{ role: 'user', content: prompt }], max_tokens: 800 });
      const text = (r?.response ?? '').replace(/```json|```/g, '').trim();
      const json = text.slice(text.indexOf('{'), text.lastIndexOf('}') + 1);
      const parsed = JSON.parse(json) as { assignments: Record<string, string> };
      Object.assign(assignments, parsed.assignments);
    } catch {
      batch.forEach((it, i) => { assignments[start + i] = typeToTheme(it.type); });
    }
  }

  const bucketMap = new Map<string, NormalizedItem[]>();
  items.forEach((item, i) => {
    const theme = THEMES.find(t => t.key === assignments[i])?.key ?? typeToTheme(item.type);
    if (!bucketMap.has(theme)) bucketMap.set(theme, []);
    bucketMap.get(theme)!.push(item);
  });

  return THEMES
    .map(theme => {
      const raw = bucketMap.get(theme.key);
      if (!raw?.length) return null;
      const mixed = interleavePlatforms(raw);
      return { theme: theme.key, label: theme.label, emoji: theme.emoji, items: mixed.slice(0, 20), hasMore: mixed.length > 20 };
    })
    .filter(Boolean) as ThematicBucket[];
}

// ─── Handler ──────────────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const cors = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS' };
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });

    const { pathname, searchParams } = new URL(request.url);
    if (pathname !== '/home') {
      return new Response('{"endpoints":["/home","/home?refresh"]}', { headers: { ...cors, 'Content-Type': 'application/json' } });
    }

    try {
      // Cache
      if (!searchParams.has('refresh')) {
        const cached = await env.KV.get('home_data').catch(() => null);
        if (cached) return new Response(cached, { headers: { ...cors, 'Content-Type': 'application/json', 'X-Cache': 'HIT' } });
      }

      const [rtlItems, rtbfItems, tf1Items, banners] = await Promise.all([
        fetchRTL().catch(() => [] as NormalizedItem[]),
        fetchRTBF().catch(() => [] as NormalizedItem[]),
        fetchTF1().catch(() => [] as NormalizedItem[]),
        fetchHeroBanners().catch(() => [] as any[]),
      ]);

      // Dédoublonnage
      const seen = new Set<string>();
      const all  = [...rtlItems, ...rtbfItems, ...tf1Items].filter(it => {
        const k = it.title.toLowerCase().replace(/\s+/g, '');
        return seen.has(k) ? false : (seen.add(k), true);
      });

      const buckets = await aiClassify(all, env.AI);

      const body = JSON.stringify({
        heroBanners: banners,
        buckets,
        meta: { rtl: rtlItems.length, rtbf: rtbfItems.length, tf1: tf1Items.length, total: all.length, buckets: buckets.length, generatedAt: new Date().toISOString() },
      });

      await env.KV.put('home_data', body, { expirationTtl: CACHE_TTL }).catch(() => {});
      return new Response(body, { headers: { ...cors, 'Content-Type': 'application/json', 'X-Cache': 'MISS' } });

    } catch (err) {
      return new Response(JSON.stringify({ error: String(err) }), { status: 500, headers: { ...cors, 'Content-Type': 'application/json' } });
    }
  },
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

function guessTypeFromLabels(labels: any[]): string {
  if (!labels) return 'SHOW';
  const s = labels.map((l: any) => l.label ?? l).join(' ').toLowerCase();
  if (/film|movie/.test(s)) return 'FILM';
  if (/série|serie|saison/.test(s)) return 'SERIE';
  return 'SHOW';
}

function tf1Type(categories: any[]): string {
  const s = (categories ?? []).map((c: any) => c.label ?? '').join(' ').toLowerCase();
  if (/film/.test(s)) return 'FILM';
  if (/série/.test(s)) return 'SERIE';
  return 'SHOW';
}

function typeToTheme(type: string): string {
  if (type === 'FILM')  return 'films';
  if (type === 'SERIE') return 'series';
  return 'trending';
}

function interleavePlatforms(items: NormalizedItem[]): NormalizedItem[] {
  const m: Record<string, NormalizedItem[]> = {};
  for (const it of items) { if (!m[it.platform]) m[it.platform] = []; m[it.platform].push(it); }
  const ps = Object.keys(m);
  const out: NormalizedItem[] = [];
  for (let i = 0; out.length < items.length; i++) {
    const arr = m[ps[i % ps.length]];
    if (arr?.length) out.push(arr.shift()!);
    if (ps.every(p => !m[p]?.length)) break;
  }
  return out;
}
