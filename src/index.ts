/**
 * Cloudflare Worker — Aggregateur multi-plateforme StreamHub
 *
 * GET /home              → { buckets, heroBanners, meta }
 * GET /list?theme=<key>  → { theme, label, emoji, items[], meta }
 *
 * Bindings requis (wrangler.toml) :
 *   [ai]              binding = "AI"
 *   [[kv_namespaces]] binding = "LABEL_CACHE"
 */

export interface Env {
  AI: Ai;
  LABEL_CACHE: KVNamespace;
}

// ─── Types ────────────────────────────────────────────────────────────────────

type ThemeKey =
  | 'top' | 'sooner' | 'episodes' | 'thriller' | 'films' | 'series'
  | 'documentaire' | 'culture' | 'info' | 'sport' | 'kids' | 'telerealite';

interface NormalizedItem {
  id: string;
  title: string;
  subtitle?: string;
  description?: string;
  illustration?: Record<string, string>;
  duration?: number;
  categoryLabel?: string;
  platform: 'RTBF' | 'TF1+';
  channelLabel?: string;
  resourceType?: string;
  path?: string;
  rating?: string | null;
  theme: ThemeKey;
  _raw: any;
}

interface ThematicBucket {
  theme: ThemeKey;
  label: string;
  emoji: string;
  items: NormalizedItem[];
  hasMore: boolean;
}

// ─── Maps de classification ───────────────────────────────────────────────────

const CATEGORY_MAP: Record<string, ThemeKey> = {
  'policier': 'thriller', 'affaires criminelles': 'thriller', 'crime': 'thriller',
  'thriller': 'thriller', 'serie policiere': 'thriller', 'police': 'thriller',
  'suspense': 'thriller', 'horreur': 'thriller', 'espionnage': 'thriller', 'polar': 'thriller',
  'film': 'films', 'films': 'films', 'comedie': 'films', 'comedie dramatique': 'films',
  'action': 'films', 'aventure': 'films', 'science-fiction': 'films', 'sf': 'films',
  'fantastique': 'films', 'animation': 'films', 'romance': 'films', 'western': 'films',
  'biopic': 'films', 'telefilm': 'films', 'drame': 'films', 'cinema': 'films',
  'comedie romantique': 'films',
  'documentaire': 'documentaire', 'investigation': 'documentaire', 'societe': 'documentaire',
  'histoire': 'documentaire', 'decouvertes': 'documentaire', 'reportage': 'documentaire',
  'nature': 'documentaire', 'science': 'documentaire', 'environnement': 'documentaire',
  'voyage': 'documentaire', 'monde': 'documentaire', 'enquete': 'documentaire',
  'culture': 'culture', 'divertissement': 'culture', 'humour': 'culture',
  'musique': 'culture', 'talk show': 'culture', 'varietes': 'culture',
  'magazine': 'culture', 'lifestyle': 'culture', 'spectacle': 'culture', 'concert': 'culture',
  'people': 'culture', 'litterature': 'culture', 'jeux': 'culture',
  'emission': 'culture', 'game show': 'culture', 'quiz': 'culture',
  'talk-show': 'culture', 'variete': 'culture',
  'info': 'info', 'actualite': 'info', 'actualites': 'info', 'journal': 'info',
  'politique': 'info', 'economie': 'info', 'news': 'info', 'debat': 'info',
  'information': 'info', "magazine d'info": 'info',
  'telerealite': 'telerealite', 'docu-realite': 'telerealite', 'docureality': 'telerealite',
  'real': 'telerealite', 'reality': 'telerealite', 'aventure / survie': 'telerealite',
  'kids': 'kids', 'enfants': 'kids', 'jeunesse': 'kids', 'anime': 'kids', 'dessin anime': 'kids',
  'sport': 'sport', 'football': 'sport', 'cyclisme': 'sport', 'tennis': 'sport',
  'rugby': 'sport', 'formule 1': 'sport', 'athletisme': 'sport', 'moteurs': 'sport',
  'basket': 'sport', 'natation': 'sport', 'f1': 'sport', 'moto': 'sport',
  'golf': 'sport', 'boxe': 'sport',
  'serie': 'series', 'sitcom': 'series', 'feuilleton': 'series', 'mini serie': 'series',
};

const TF1_TOPICS_MAP: Record<string, ThemeKey> = {
  'telerealite': 'telerealite', 'docu-realite': 'telerealite', 'survie': 'telerealite',
  'mariage': 'telerealite', 'famille': 'telerealite', 'lifestyle': 'telerealite',
  'danse': 'culture', 'chanson': 'culture', 'divertissement': 'culture',
  'musique': 'culture', 'concert': 'culture', 'spectacle': 'culture',
  'quiz': 'culture', 'humour': 'culture', 'culture': 'culture', 'talk show': 'culture',
  'sport': 'sport', 'football': 'sport', 'cyclisme': 'sport', 'rugby': 'sport',
  'athletisme': 'sport', 'docu-realite sportive': 'sport',
  'actualite': 'info', 'journal televise': 'info', 'faits divers': 'info',
  'reportages': 'documentaire', 'enquete': 'documentaire', 'nature': 'documentaire',
  'histoire': 'documentaire',
  'policier': 'thriller', 'thriller': 'thriller', 'suspense': 'thriller', 'crime': 'thriller',
  'action': 'films', 'aventure': 'films', 'drame': 'films', 'comedie': 'films',
  'romance': 'films', 'fantastique': 'films',
};

const THEMES: Record<ThemeKey, { label: string; emoji: string }> = {
  top:          { label: 'Top TF1+',                emoji: '⭐' },
  sooner:       { label: 'Sooner',                  emoji: '🎬' },
  episodes:     { label: 'Épisodes récents',         emoji: '🎞️' },
  thriller:     { label: 'Policier & Thriller',      emoji: '🔍' },
  films:        { label: 'Films',                    emoji: '🎬' },
  series:       { label: 'Séries',                   emoji: '📺' },
  documentaire: { label: 'Documentaires',            emoji: '📽️' },
  culture:      { label: 'Culture & Divertissement', emoji: '🎭' },
  info:         { label: 'Info & Actualités',        emoji: '📰' },
  sport:        { label: 'Sport',                    emoji: '⚽' },
  kids:         { label: 'Kids',                     emoji: '🌟' },
  telerealite:  { label: 'Téléréalité',              emoji: '🎪' },
};

const BUCKET_ORDER: ThemeKey[] = [
  'top', 'episodes', 'thriller', 'films', 'series', 'telerealite',
  'documentaire', 'culture', 'info', 'sport', 'kids', 'sooner',
];

const LANDSCAPE_BUCKETS = new Set<ThemeKey>([
  'episodes', 'thriller', 'documentaire', 'culture', 'info', 'sport',
]);

const THEMES_WITH_LIST = new Set<ThemeKey>([
  'films', 'series', 'documentaire', 'culture', 'info', 'sport',
  'kids', 'sooner', 'telerealite', 'thriller', 'episodes',
]);

// ─── Config /list ─────────────────────────────────────────────────────────────

/**
 * RTBF : page catégorie (multiples widgets paginés) ou widget(s) direct(s)
 */
const RTBF_LIST_CONFIG: Partial<Record<ThemeKey, {
  type: 'category'; path: string;
} | {
  type: 'widgets'; ids: string[]; forceTitle?: string;
}>> = {
  films:        { type: 'category', path: 'films-36' },
  series:       { type: 'category', path: 'series-35' },
  documentaire: { type: 'category', path: 'documentaires-31' },
  info:         { type: 'category', path: 'info-1' },
  sport:        { type: 'category', path: 'sport-9' },
  kids:         { type: 'widgets', ids: ['22390'], forceTitle: 'Kids' },
  sooner:       { type: 'widgets', ids: ['19737'] },
  culture:      { type: 'widgets', ids: ['20136', '20691'] },
  // Partagent la page séries, filtrés post-normalisation
  episodes:     { type: 'category', path: 'series-35' },
  thriller:     { type: 'category', path: 'series-35' },
  telerealite:  { type: 'category', path: 'series-35' },
};

/**
 * TF1 : query GraphQL fr-be categoryBySlug avec le BON query ID (46f87e88...).
 * Ce query retourne covers[] ET sliders[].items[] — on extrait les deux.
 *
 * Slugs disponibles (confirmés) :
 *   series, films, telefilms, divertissement, sport, info,
 *   reportages, jeunesse, people-43944072, podcasts-70045207
 */
const TF1_LIST_CONFIG: Partial<Record<ThemeKey, { slugs: string[] }>> = {
  films:        { slugs: ['films', 'telefilms'] },
  series:       { slugs: ['series'] },
  documentaire: { slugs: ['reportages'] },
  telerealite:  { slugs: ['divertissement'] },
  culture:      { slugs: ['divertissement', 'people-43944072'] },
  thriller:     { slugs: ['series'] },
  episodes:     { slugs: ['series'] },
  sooner:       { slugs: ['films', 'telefilms'] },
  info:         { slugs: ['info'] },
  sport:        { slugs: ['sport'] },
  kids:         { slugs: ['jeunesse'] },
};

// ─── Endpoints TF1 ───────────────────────────────────────────────────────────

const TF1_GRAPHQL_BASE = 'https://www.tf1.fr/graphql/fr-be/web';

/**
 * Query ID pour categoryBySlug avec sliders (capturé depuis les network requests TF1).
 * Ce query retourne : covers[], sliders[{ id, title, items[] }]
 * Différent du 8cc10401 qui ne retourne que les covers.
 */
const TF1_CATEGORY_SLIDERS_QID = '46f87e88577a61abb1d2a36a715a12d4175caa3d';

/** Query ID de la home (homeSliders) */
const TF1_HOME_QID   = 'c34093152db844db6b7ad9b56df12841f7d13182';
const TF1_BANNER_QID = 'bd8e6aab9996844dad4ea9a53887adad27d86151';

// ─── Classification ───────────────────────────────────────────────────────────

function normalizeLabel(s: string): string {
  return s.toLowerCase().trim()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ');
}

function resolveTheme(
  categoryLabel: string | undefined,
  topics: string[] | undefined,
  typology: string | undefined,
  durationSec: number | undefined,
  llmCache: Record<string, ThemeKey>,
): ThemeKey {
  const GENERIC = new Set(['emission', 'spectacle']);
  const REALITE  = new Set(['telerealite', 'docu-realite', 'mariage', 'famille', 'lifestyle', 'survie', 'aventure']);

  // 1. Typologies génériques TF1 → regarder les topics d'abord
  if (typology && GENERIC.has(normalizeLabel(typology)) && topics?.length) {
    for (const t of topics) { if (REALITE.has(normalizeLabel(t))) return 'telerealite'; }
    for (const t of topics) {
      const k = normalizeLabel(t);
      if (TF1_TOPICS_MAP[k] && TF1_TOPICS_MAP[k] !== 'films') return TF1_TOPICS_MAP[k];
      if (CATEGORY_MAP[k] && CATEGORY_MAP[k] !== 'culture' && CATEGORY_MAP[k] !== 'films') return CATEGORY_MAP[k];
    }
    for (const t of topics) { const k = normalizeLabel(t); if (TF1_TOPICS_MAP[k]) return TF1_TOPICS_MAP[k]; }
    return 'culture';
  }

  // 2. Typology précise
  if (typology) {
    const k = normalizeLabel(typology);
    const m = CATEGORY_MAP[k];
    if (m) {
      if (['drame', 'comedie', 'comedie dramatique'].includes(k) && durationSec !== undefined)
        return durationSec > 4800 ? 'films' : 'series';
      return m;
    }
  }

  // 3. CategoryLabel
  if (categoryLabel) {
    const key = normalizeLabel(categoryLabel);
    if (key && !GENERIC.has(key)) {
      const m = CATEGORY_MAP[key];
      if (m) {
        if (['drame', 'comedie', 'comedie dramatique'].includes(key) && durationSec !== undefined)
          return durationSec > 4800 ? 'films' : 'series';
        return m;
      }
      for (const [f, th] of Object.entries(CATEGORY_MAP)) { if (key.startsWith(f)) return th; }
      for (const [f, th] of Object.entries(CATEGORY_MAP)) { if (f.length >= 6 && key.includes(f)) return th; }
      if (llmCache[key]) return llmCache[key];
    }
  }

  // 4. Topics (cas général)
  if (topics?.length) {
    for (const t of topics) {
      const k = normalizeLabel(t);
      if (TF1_TOPICS_MAP[k]) return TF1_TOPICS_MAP[k];
      if (CATEGORY_MAP[k]) return CATEGORY_MAP[k];
    }
  }

  return 'series';
}

// ─── Workers AI ───────────────────────────────────────────────────────────────

const LOCAL_FALLBACK: Record<string, ThemeKey> = {
  'conte': 'kids', 'fable': 'kids', 'marionnettes': 'kids',
  'short': 'films', 'court metrage': 'films',
  'catchup': 'culture', 'replay': 'culture',
  'emission sportive': 'sport', 'magazine sportif': 'sport',
  'actu': 'info', 'flash info': 'info', 'meteo': 'info',
  'sante': 'documentaire', 'medical': 'documentaire',
  'cuisine': 'documentaire', 'gastronomie': 'documentaire',
  'mode': 'culture', 'deco': 'culture', 'talkshow': 'culture', 'late show': 'culture',
  'docu-serie': 'documentaire', 'serie documentaire': 'documentaire',
  'serie animee': 'kids', 'animation jeunesse': 'kids',
  'comedie romantique': 'films', 'thriller psychologique': 'thriller',
  'serie policiere': 'thriller', 'policiere': 'thriller',
  'aventure sportive': 'sport', 'sport mecanique': 'sport',
  'magazine people': 'culture', 'emission musicale': 'culture',
};

async function classifyWithWorkersAI(labels: string[], env: Env): Promise<Record<string, ThemeKey>> {
  if (!labels.length) return {};
  const resolved: Record<string, ThemeKey> = {};
  const unknown: string[] = [];

  for (const lbl of labels) {
    const k = normalizeLabel(lbl);
    if (LOCAL_FALLBACK[k]) { resolved[k] = LOCAL_FALLBACK[k]; continue; }
    let found: ThemeKey | null = null;
    for (const [f, t] of Object.entries(LOCAL_FALLBACK)) {
      if (k.includes(f) || f.includes(k)) { found = t; break; }
    }
    if (found) resolved[k] = found; else unknown.push(lbl);
  }

  if (!unknown.length) return resolved;

  try {
    const res = await env.AI.run('@cf/meta/llama-3-8b-instruct', {
      messages: [{ role: 'user', content: `Tu es un classificateur de genres vidéo.\nPour chaque label, retourne le thème parmi : ${Object.keys(THEMES).join(', ')}.\nJSON uniquement. Format : { "label": "theme" }\n\n${unknown.map(l => `- "${l}"`).join('\n')}` }],
      max_tokens: 512,
    });
    const m = ((res as any).response ?? '').match(/\{[\s\S]*\}/);
    if (m) {
      for (const [lbl, th] of Object.entries(JSON.parse(m[0]))) {
        if (th in THEMES) resolved[normalizeLabel(lbl)] = th as ThemeKey;
      }
    }
  } catch { for (const lbl of unknown) resolved[normalizeLabel(lbl)] = 'series'; }

  return resolved;
}

// ─── Fetch RTBF ───────────────────────────────────────────────────────────────

async function fetchRTBF(): Promise<any> {
  const res = await fetch('https://bff-service.rtbf.be/auvio/v1.23/pages/home?userAgent=Chrome-web-3.0', {
    headers: { Accept: 'application/json' },
  });
  if (!res.ok) throw new Error(`RTBF home ${res.status}`);
  return res.json();
}

/**
 * Fetch une page d'un widget RTBF.
 * Retourne { items, next } — next est l'URL de la page suivante (links.next) ou null.
 */
async function fetchRTBFPage(url: string): Promise<{ items: any[]; next: string | null }> {
  try {
    const full = url.startsWith('http') ? url : `https://bff-service.rtbf.be${url}`;
    const res  = await fetch(full, { headers: { Accept: 'application/json' } });
    if (!res.ok) return { items: [], next: null };
    const json: any = await res.json();
    const items: any[] = json?.data?.content ?? json?.data ?? [];
    const rawNext: string | null = json?.links?.next ?? null;
    const next = rawNext
      ? (rawNext.startsWith('http') ? rawNext : `https://bff-service.rtbf.be${rawNext}`)
      : null;
    return { items, next };
  } catch { return { items: [], next: null }; }
}

/**
 * Pagine entièrement un widget RTBF (jusqu'à maxPages).
 * Première URL : _limit=48 ajouté automatiquement.
 */
async function fetchRTBFWidgetAll(contentPath: string, maxPages = 5): Promise<any[]> {
  const base     = contentPath.startsWith('http') ? contentPath : `https://bff-service.rtbf.be${contentPath}`;
  const firstUrl = `${base}${base.includes('?') ? '&' : '?'}_limit=48&_embed=content`;
  const all: any[] = [];
  let url: string | null = firstUrl;
  let page = 0;

  while (url && page < maxPages) {
    const { items, next } = await fetchRTBFPage(url);
    all.push(...items);
    url = next;
    page++;
    if (!items.length) break;
  }
  return all;
}

/**
 * Fetch une page catégorie RTBF et retourne TOUS les items de tous ses widgets (paginés).
 * Ex : /categorie/films-36 → plusieurs PROGRAM_LIST et MEDIA_LIST, chacun paginé.
 */
async function fetchRTBFCategoryAll(categoryPath: string): Promise<{ items: any[]; widgetTitle: string }[]> {
  const url = `https://bff-service.rtbf.be/auvio/v1.23/pages/categorie/${categoryPath}?userAgent=Chrome-web-3.0`;
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!res.ok) return [];

  const json: any = await res.json();
  const EXCL = new Set([
    'FAVORITE_PROGRAM_LIST', 'CHANNEL_LIST', 'ONGOING_PLAY_HISTORY',
    'CATEGORY_LIST', 'BANNER', 'MEDIA_TRAILER', 'PROMOBOX',
  ]);
  const widgets = (json?.data?.widgets ?? []).filter((w: any) => !EXCL.has(w.type) && w.contentPath);

  // Paginer chaque widget en parallèle (max 4 pages × 48 = ~192 items/widget)
  const results = await Promise.allSettled(
    widgets.map((w: any) =>
      fetchRTBFWidgetAll(w.contentPath, 4)
        .then(items => ({ items, widgetTitle: w.title ?? '' })),
    ),
  );

  return results
    .filter((r): r is PromiseFulfilledResult<{ items: any[]; widgetTitle: string }> => r.status === 'fulfilled')
    .map(r => r.value);
}

// ─── Fetch TF1 categoryBySlug (nouveau query ID avec sliders) ────────────────

/**
 * Fetch la catégorie TF1 avec le query 46f87e88 qui retourne :
 *   categoryBySlug {
 *     covers[]          → banners éditoriaux (CoverOfProgram, CoverOfExternalLink…)
 *     sliders[] {
 *       id, title
 *       items[]         → les VRAIS programmes/vidéos de la catégorie
 *     }
 *   }
 *
 * On extrait les items de TOUS les sliders pour avoir le catalogue complet.
 * Les covers sont aussi extraits (ils contiennent des programmes valides).
 */
async function fetchTF1CategorySliders(slugs: string[]): Promise<any[]> {
  const headers = {
    'content-type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Origin': 'https://www.tf1.fr',
    'Referer': 'https://www.tf1.fr/',
  };

  const allItems: any[] = [];

  for (const slug of slugs) {
    const variables = encodeURIComponent(JSON.stringify({
      categorySlug: slug,
      limit: 50,
      ofContentTypes: [
        'ARTICLE', 'CATEGORY', 'CHANNEL', 'COLLECTION', 'EXTERNAL_LINK', 'LANDING_PAGE',
        'LIVE', 'NEXT_BROADCAST', 'PERSONALITY', 'PLAYLIST', 'PLUGIN', 'PROGRAM',
        'PROGRAM_BY_CATEGORY', 'SMART_SUMMARY', 'TOP_PROGRAM', 'TOP_VIDEO', 'TRAILER', 'VIDEO',
      ],
      ofBannerTypes: ['LARGE', 'MEDIUM'],
      ofChannelTypes: ['CORNER', 'DIGITAL', 'EVENT', 'PARTNER', 'TV'],
    }));

    try {
      const res = await fetch(
        `${TF1_GRAPHQL_BASE}?id=${TF1_CATEGORY_SLIDERS_QID}&variables=${variables}`,
        { method: 'GET', headers },
      );

      if (!res.ok) {
        console.error(`[TF1 cat] ${res.status} slug=${slug}`);
        continue;
      }

      const json: any = await res.json();
      const cat = json?.data?.categoryBySlug ?? {};

      let coversCount = 0, slidersItemsCount = 0;

      // ── covers[] ──────────────────────────────────────────────────────────
      // Chaque cover est un CoverOfProgram | CoverOfExternalLink | CoverOfVideo
      // On ne prend que ceux qui ont un programme associé (pas les liens externes)
      for (const cover of (cat.covers ?? [])) {
        if (cover.__typename === 'CoverOfExternalLink') continue; // pas de contenu vidéo
        if (cover.program || cover.video || cover.__typename === 'CoverOfProgram') {
          allItems.push(cover);
          coversCount++;
        }
      }

      // ── sliders[].items[] ─────────────────────────────────────────────────
      // C'est ici que sont les VRAIS programmes de la catégorie
      for (const slider of (cat.sliders ?? [])) {
        const sliderItems: any[] = slider.items ?? slider.programs ?? [];
        allItems.push(...sliderItems);
        slidersItemsCount += sliderItems.length;
        console.log(`[TF1 cat] slug=${slug} slider="${slider.title ?? slider.id}" items=${sliderItems.length}`);
      }

      console.log(`[TF1 cat] slug=${slug} covers=${coversCount} sliderItems=${slidersItemsCount} total=${allItems.length}`);
    } catch (err) {
      console.error(`[TF1 cat] error slug=${slug}:`, err);
    }
  }

  return allItems;
}

// ─── Fetch TF1 Home ───────────────────────────────────────────────────────────

async function fetchTF1(): Promise<any> {
  const headers = {
    'content-type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Origin': 'https://www.tf1.fr',
    'Referer': 'https://www.tf1.fr/',
  };
  const homeParams = encodeURIComponent(JSON.stringify({
    ofBannerTypes: ['LARGE', 'MEDIUM'],
    ofContentTypes: [
      'ARTICLE', 'CATEGORY', 'CHANNEL', 'COLLECTION', 'EXTERNAL_LINK', 'LANDING_PAGE',
      'LIVE', 'NEXT_BROADCAST', 'PERSONALITY', 'PLAYLIST', 'PLUGIN', 'PROGRAM',
      'PROGRAM_BY_CATEGORY', 'SMART_SUMMARY', 'TOP_PROGRAM', 'TOP_VIDEO', 'TRAILER', 'VIDEO',
    ],
    ofChannelTypes: ['CORNER', 'DIGITAL', 'EVENT', 'PARTNER', 'TV'],
  }));

  const [homeRes, bannersRes] = await Promise.allSettled([
    fetch(`${TF1_GRAPHQL_BASE}?id=${TF1_HOME_QID}&variables=${homeParams}`, {
      method: 'GET', headers,
    }),
    fetch(`https://www.tf1.fr/graphql/web?id=${TF1_BANNER_QID}`, {
      method: 'GET', headers,
    }),
  ]);

  const homeJson    = homeRes.status === 'fulfilled' && homeRes.value.ok
    ? await homeRes.value.json() : null;
  const bannersJson = bannersRes.status === 'fulfilled' && bannersRes.value.ok
    ? await bannersRes.value.json() : null;

  return { ...(homeJson ?? {}), _tf1Banners: bannersJson?.data?.homeCoversByRight ?? [] };
}

// ─── Normalisation RTBF ───────────────────────────────────────────────────────

function normalizeRTBFItem(
  item: any,
  llmCache: Record<string, ThemeKey>,
  widgetTitle = '',
): NormalizedItem | null {
  if (!item || item.resourceType === 'LIVE') return null;

  const wl = widgetTitle.toLowerCase();
  const isKids   = wl.includes('kids') || wl.includes('enfant') || wl.includes('jeunesse');
  const isSooner = item.resourceType === 'MEDIA_PREMIUM'
    || (Array.isArray(item.products) && item.products.some((p: any) => p.label === 'Sooner'));

  const baseTheme = isSooner ? 'sooner'
    : isKids ? 'kids'
    : resolveTheme(item.categoryLabel, undefined, undefined, item.duration, llmCache);

  const isEpisode = item.type === 'VIDEO' && item.resourceType === 'MEDIA';
  const episodeBuckets = new Set<ThemeKey>(['series', 'films', 'thriller', 'telerealite']);
  const theme = (isEpisode && episodeBuckets.has(baseTheme)) ? 'episodes' : baseTheme;

  return {
    id:            `rtbf-${item.id ?? item.assetId}`,
    title:         item.title ?? '',
    subtitle:      item.subtitle,
    description:   item.description,
    illustration:  item.illustration,
    duration:      item.duration,
    categoryLabel: item.categoryLabel,
    platform:      'RTBF',
    channelLabel:  item.channelLabel,
    resourceType:  item.resourceType,
    path:          item.path,
    rating:        item.rating,
    theme,
    _raw: item,
  };
}

// ─── Normalisation TF1 ────────────────────────────────────────────────────────

function pickBestUrl(sources: any[]): string | undefined {
  if (!sources?.length) return undefined;
  return [...sources]
    .sort((a, b) => (b.scale ?? 0) - (a.scale ?? 0))
    .find(s => s.type === 'jpg' || s.type === 'webp' || s.url)?.url;
}

/**
 * Construit { xs, s, m, l, xl } depuis sourcesWithScales — identique à
 * createIllustrationFromSources de tf1plus-api.ts, utilisé partout dans VideoCard.
 */
function buildIllustration(sources: any[]): Record<string, string> | undefined {
  if (!sources?.length) return undefined;
  const webp = sources.filter(s => s.type === 'webp');
  const jpg  = sources.filter(s => s.type === 'jpg' || s.type === 'jpeg');
  const pool = webp.length ? webp : (jpg.length ? jpg : sources);
  const sorted = [...pool].sort((a, b) => (b.scale ?? 0) - (a.scale ?? 0));
  const s3 = sorted.filter(s => s.scale === 3);
  const s2 = sorted.filter(s => s.scale === 2);
  const s1 = sorted.filter(s => s.scale === 1);
  const best = sorted[0]?.url;
  if (!best) return undefined;
  return {
    xl: s3[0]?.url ?? s2[0]?.url ?? best,
    l:  s3[0]?.url ?? s2[0]?.url ?? best,
    m:  s2[0]?.url ?? s3[0]?.url ?? best,
    s:  s2[0]?.url ?? s1[0]?.url ?? best,
    xs: s1[0]?.url ?? s2[0]?.url ?? best,
  };
}

/**
 * Normalise un item TF1 quelle que soit sa source :
 *   - homeSliders items (SliderItem, TopProgramItem, Video…)
 *   - categoryBySlug covers (CoverOfProgram, CoverOfVideo)
 *   - categoryBySlug sliders items (Program, Video, ProgramItem…)
 */
function normalizeTF1Item(
  item: any,
  llmCache: Record<string, ThemeKey>,
): NormalizedItem | null {
  if (!item) return null;

  // ── Identité ────────────────────────────────────────────────────────────────
  // Un CoverOfProgram a item.program; un item de slider peut être un Program direct
  const prog  = item.program ?? item;
  const id    = prog.id ?? item.id;
  const title = prog.decoration?.label ?? prog.name
    ?? item.decoration?.label ?? item.name ?? item.label ?? '';
  if (!title || !id) return null;

  const typology: string = prog.typology ?? item.typology ?? '';
  const topics: string[] = prog.topics   ?? item.program?.topics ?? [];
  const duration         = item.duration ?? prog.duration ?? 0;
  const rawCategory      = typology || (item.__typename === 'Video' ? 'Divertissement' : '');

  const isTopProgram = item.__typename === 'TopProgramItem';
  const theme: ThemeKey = isTopProgram ? 'top'
    : resolveTheme(rawCategory || undefined, topics, typology || undefined, duration, llmCache);

  // ── Images ──────────────────────────────────────────────────────────────────
  const useLandscape = LANDSCAPE_BUCKETS.has(theme);

  // Sources portrait (2/3)
  const portraitSrc =
    prog.decoration?.portrait?.sourcesWithScales ??
    item.decoration?.portrait?.sourcesWithScales ??
    item.thumbnail?.sourcesWithScales;

  // Sources landscape (16/9)
  const landscapeSrc =
    prog.decoration?.thumbnail?.sourcesWithScales ??
    item.decoration?.thumbnail?.sourcesWithScales ??
    item.image?.sourcesWithScales ??
    prog.decoration?.background?.sourcesWithScales;

  // Sources coverSmall pour les CoverOfProgram (portrait haute résolution)
  const coverSmallSrc =
    item.decoration?.coverSmall?.sourcesWithScales ??
    item.coverSmall?.sourcesWithScales;

  // Sources cover pour les CoverOfProgram (landscape haute résolution)
  const coverSrc =
    item.decoration?.cover?.sourcesWithScales ??
    item.cover?.sourcesWithScales;

  const portraitIllus  = buildIllustration(coverSmallSrc ?? portraitSrc  ?? []);
  const landscapeIllus = buildIllustration(coverSrc      ?? landscapeSrc ?? []);

  let illustration = useLandscape
    ? (landscapeIllus ?? portraitIllus)
    : (portraitIllus  ?? landscapeIllus);

  // Fallback URL unique
  if (!illustration) {
    const fallbackUrl = pickBestUrl([
      ...(portraitSrc  ?? []),
      ...(landscapeSrc ?? []),
      ...(coverSmallSrc ?? []),
      ...(coverSrc      ?? []),
    ]);
    if (fallbackUrl) {
      illustration = { xs: fallbackUrl, s: fallbackUrl, m: fallbackUrl, l: fallbackUrl, xl: fallbackUrl };
    }
  }

  // ── resourceType / IDs ──────────────────────────────────────────────────────
  const isVideo = item.__typename === 'Video';
  const isFilm  = typology === 'Film' || typology === 'Téléfilm';
  const resourceType: 'PROGRAM' | 'MEDIA' = (isVideo || isFilm) ? 'MEDIA' : 'PROGRAM';

  // Pour les CoverOfProgram avec Film : le CTA donne le vrai videoId
  const ctaVideoId = item.callToAction?.items?.find(
    (i: any) => i.type === 'PLAY' || i.__typename === 'WatchButtonAction',
  )?.video?.id ?? null;
  const mediaId = isVideo ? id : (isFilm ? (ctaVideoId ?? id) : undefined);

  // ── Badges ──────────────────────────────────────────────────────────────────
  let rating: string | null = prog.rating ?? item.rating ?? null;
  if (rating) rating = rating.replace('CSA_', '').replace('ALL', 'Tout public');

  const allBadges = [...(item.badges ?? []), ...(item.editorBadges ?? [])];
  const stamp = allBadges.length > 0
    ? { label: allBadges[0].label ?? allBadges[0].type ?? '', backgroundColor: '#1a56db', textColor: '#fff' }
    : undefined;

  // ── _raw enrichi (identique à l'existant, compatible VideoCard) ─────────────
  const enrichedRaw = {
    ...item,
    id, title,
    subtitle:    prog.decoration?.catchPhrase ?? item.decoration?.catchPhrase,
    description: prog.synopsis ?? prog.decoration?.description ?? item.synopsis,
    illustration,
    duration, typology,
    slug:        prog.slug ?? item.slug,
    programId:   prog.id,
    programSlug: prog.slug,
    resourceType,
    platform:    'TF1+',
    streamId:    mediaId,
    assetId:     mediaId,
    hasSubtitles:        (prog.hasFrenchDeafSubtitles?.total ?? 0) > 0
                      || (prog.hasFrenchSubtitles?.total      ?? 0) > 0,
    hasAudioDescriptions: (prog.hasDescriptionTrack?.total ?? 0) > 0,
    rating, stamp,
    ...(isFilm ? { isFilm: true } : {}),
  };

  return {
    id:           `tf1-${id}`,
    title,
    subtitle:     enrichedRaw.subtitle,
    description:  enrichedRaw.description,
    illustration,
    duration,
    categoryLabel: rawCategory || typology || 'TF1+',
    platform:     'TF1+',
    channelLabel: prog.mainChannel?.slug ?? 'TF1+',
    resourceType,
    path: `/tf1/${resourceType === 'MEDIA' ? 'video' : 'program'}/${mediaId ?? id}`,
    theme,
    _raw: enrichedRaw,
  };
}

// ─── Déduplication ────────────────────────────────────────────────────────────

function deduplicate(items: NormalizedItem[]): NormalizedItem[] {
  const seen = new Map<string, NormalizedItem>();
  for (const item of items) {
    const key = `${item.platform}:${item.title.toLowerCase().trim().replace(/\s+/g, ' ')}`;
    if (!seen.has(key)) seen.set(key, item);
  }
  return Array.from(seen.values());
}

// ─── Build buckets ────────────────────────────────────────────────────────────

function buildBuckets(
  rtbfItems: NormalizedItem[],
  tf1Items: NormalizedItem[],
): ThematicBucket[] {
  const rG = new Map<ThemeKey, NormalizedItem[]>();
  const tG = new Map<ThemeKey, NormalizedItem[]>();
  for (const th of BUCKET_ORDER) { rG.set(th, []); tG.set(th, []); }

  for (const i of rtbfItems) (rG.get(i.theme) ?? rG.set(i.theme, []).get(i.theme)!).push(i);
  for (const i of tf1Items)  (tG.get(i.theme) ?? tG.set(i.theme, []).get(i.theme)!).push(i);

  return BUCKET_ORDER.map(theme => {
    const rtbf = rG.get(theme) ?? [], tf1 = tG.get(theme) ?? [];
    const merged: NormalizedItem[] = [];
    let r = 0, t = 0;
    while (r < rtbf.length || t < tf1.length) {
      if (r < rtbf.length) merged.push(rtbf[r++]);
      if (t < tf1.length)  merged.push(tf1[t++]);
    }
    return {
      theme, label: THEMES[theme].label, emoji: THEMES[theme].emoji,
      items: merged, hasMore: THEMES_WITH_LIST.has(theme),
    };
  }).filter(b => b.items.length > 0);
}

// ─── Banners ──────────────────────────────────────────────────────────────────

function buildRTBFBanners(rtbfHome: any, promoboxItems: any[] = []): any[] {
  const banners: any[] = [];
  const items = promoboxItems.length > 0 ? promoboxItems : (() => {
    for (const w of rtbfHome?.data?.widgets ?? []) {
      if (w.type === 'PROMOBOX' && Array.isArray(w.data?.content)) return w.data.content;
    }
    return [];
  })();

  for (const d of items) {
    const dl: string = d.deeplink ?? '';
    let contentType = d.resourceType ?? 'media', contentId = String(d.resourceValue ?? d.mediaId ?? '');
    let contentSlug: string | null = null;
    const mm = dl.match(/^\/media\/(.+)-(\d+)$/);
    const em = dl.match(/^\/emission\/(.+)-(\d+)$/);
    const pm = dl.match(/^\/program(?:me)?\/(.+)-(\d+)$/);
    if (mm)      { contentType = 'media';   contentId = mm[2]; contentSlug = mm[1]; }
    else if (em) { contentType = 'program'; contentId = em[2]; contentSlug = em[1]; }
    else if (pm) { contentType = 'program'; contentId = pm[2]; contentSlug = pm[1]; }
    if (!d.title) continue;
    const image = d.image
      ? { xs: d.image.xs ?? d.image.s ?? '', s: d.image.s ?? '', m: d.image.m ?? '', l: d.image.l ?? '', xl: d.image.xl ?? d.image.l ?? '' }
      : null;
    banners.push({
      id: `rtbf-banner-${contentId || Math.random()}`, coverId: contentId,
      title: d.title, subtitle: d.subtitle ?? '', description: d.description ?? '',
      image, videoUrl: null, deepLink: dl || null, contentType, contentId, contentSlug,
      backgroundColor: d.backgroundColor ?? '#000000', platform: 'RTBF',
    });
  }
  return banners;
}

function buildTF1Banners(tf1Raw: any): any[] {
  const banners: any[] = [];
  for (const cover of (tf1Raw?._tf1Banners ?? []).slice(0, 6)) {
    const deco = cover.decoration ?? {}, prog = cover.program ?? {}, vid = cover.video ?? {};
    const id    = cover.id ?? prog.id ?? vid.id;
    const title = deco.label ?? prog.name ?? vid.program?.name ?? '';
    if (!id || !title) continue;

    const bgS  = [...(deco.cover?.sourcesWithScales     ?? [])].sort((a: any, b: any) => (b.scale ?? 1) - (a.scale ?? 1));
    const pS   = [...(deco.coverSmall?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 1) - (a.scale ?? 1));
    const bgB  = bgS.find((s: any) => s.scale >= 2)?.url ?? bgS[0]?.url ?? '';
    const bgA  = bgS[0]?.url ?? '';
    const pB   = pS.find((s: any) => s.scale >= 2)?.url ?? pS[0]?.url ?? '';
    const pA   = pS[0]?.url ?? '';
    const image = (bgB || pB)
      ? { xs: pA || bgA, s: pB || bgA, m: bgA || pA, l: bgB || pB, xl: bgB || pB }
      : null;
    const videoUrl = deco.video?.sources?.[0]?.url ?? null;
    const typename = cover.__typename ?? '';
    let contentType: string, contentId: string, contentSlug: string | null;
    const programId = prog.id ?? null, programSlug = prog.slug ?? null;

    if (typename === 'CoverOfVideo') {
      contentType = 'video'; contentId = vid.id ?? id; contentSlug = vid.slug ?? null;
    } else {
      contentType = 'program';
      const cta = cover.callToAction?.items?.find((i: any) => i.type === 'PLAY' || i.__typename === 'WatchButtonAction');
      contentId = cta?.video?.id ?? prog.id ?? id; contentSlug = cta?.video?.slug ?? prog.slug ?? null;
    }
    const ctaV   = cover.callToAction?.items?.find((i: any) => i.video)?.video ?? {};
    const topSrc = (typename === 'CoverOfVideo' ? vid.program : prog) ?? prog;

    banners.push({
      id: `tf1-banner-${id}`, coverId: String(id), title,
      description: deco.description ?? deco.catchPhrase ?? '',
      image, videoUrl, deepLink: null, contentType,
      contentId: String(contentId ?? id), contentSlug, programId, programSlug,
      typology: topSrc.typology ?? prog.typology ?? '',
      topics:   topSrc.topics   ?? prog.topics   ?? [],
      season:   vid.season   ?? ctaV?.season   ?? null,
      episode:  vid.episode  ?? ctaV?.episode  ?? null,
      duration: vid.playingInfos?.duration ?? ctaV?.playingInfos?.duration ?? null,
      rights:   vid.rights ?? ctaV?.rights ?? cover.rights ?? [],
      backgroundColor: '#000000', platform: 'TF1+',
    });
  }
  // Fallback : 5 premiers items du premier slider home
  if (!banners.length) {
    for (const item of ((tf1Raw?.data?.homeSliders ?? [])[0]?.items ?? []).slice(0, 5)) {
      const prog = item.program ?? item, id = prog.id ?? item.id;
      const title = prog.decoration?.label ?? prog.name ?? '';
      if (!id || !title) continue;
      const tS = [...(prog.decoration?.thumbnail?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 0) - (a.scale ?? 0));
      const pS = [...(prog.decoration?.portrait?.sourcesWithScales  ?? [])].sort((a: any, b: any) => (b.scale ?? 0) - (a.scale ?? 0));
      const bg = tS[0]?.url ?? '', pt = pS[0]?.url ?? '';
      banners.push({
        id: `tf1-banner-${id}`, coverId: String(id), title,
        description: prog.decoration?.catchPhrase ?? '',
        image: (bg || pt) ? { xs: pt||bg, s: pt||bg, m: bg||pt, l: bg||pt, xl: bg||pt } : null,
        videoUrl: null, deepLink: null, contentType: 'program', contentId: id,
        contentSlug: prog.slug ?? null, programId: id, programSlug: prog.slug ?? null,
        typology: prog.typology ?? '', topics: prog.topics ?? [],
        season: null, episode: null, duration: null, rights: [],
        backgroundColor: '#000000', platform: 'TF1+',
      });
    }
  }
  return banners;
}

// ─── Handler /list ────────────────────────────────────────────────────────────

async function handleListRequest(
  url: URL,
  env: Env,
  cors: Record<string, string>,
): Promise<Response> {
  const theme = url.searchParams.get('theme') as ThemeKey | null;
  if (!theme || !(theme in THEMES)) {
    return new Response(JSON.stringify({ error: 'theme invalide' }), {
      status: 400, headers: { ...cors, 'Content-Type': 'application/json' },
    });
  }

  const llmCacheRaw = await env.LABEL_CACHE.get('label_map').catch(() => null);
  const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

  const rtbfCfg = RTBF_LIST_CONFIG[theme];
  const tf1Cfg  = TF1_LIST_CONFIG[theme];

  // ── Fetch en parallèle ────────────────────────────────────────────────────
  const rtbfPromise: Promise<{ items: any[]; widgetTitle: string }[]> = rtbfCfg
    ? rtbfCfg.type === 'category'
      ? fetchRTBFCategoryAll(rtbfCfg.path)
      : Promise.all(
          rtbfCfg.ids.map(id =>
            fetchRTBFWidgetAll(
              `https://bff-service.rtbf.be/auvio/v1.23/widgets/${id}`, 6,
            ).then(items => ({ items, widgetTitle: rtbfCfg.forceTitle ?? '' })),
          ),
        )
    : Promise.resolve([]);

  const tf1Promise: Promise<any[]> = tf1Cfg
    ? fetchTF1CategorySliders(tf1Cfg.slugs)
    : Promise.resolve([]);

  const [rtbfRes, tf1Res] = await Promise.allSettled([rtbfPromise, tf1Promise]);

  // ── Normalisation RTBF ─────────────────────────────────────────────────────
  let rtbfItems: NormalizedItem[] = [];
  if (rtbfRes.status === 'fulfilled') {
    for (const { items, widgetTitle } of rtbfRes.value) {
      for (const raw of items) {
        const item = normalizeRTBFItem(raw, llmCache, widgetTitle);
        if (item) rtbfItems.push(item);
      }
    }
  }

  // ── Normalisation TF1 ─────────────────────────────────────────────────────
  let tf1Items: NormalizedItem[] = [];
  if (tf1Res.status === 'fulfilled') {
    for (const raw of tf1Res.value) {
      const item = normalizeTF1Item(raw, llmCache);
      if (item) tf1Items.push(item);
    }
  }

  // ── Filtrage thématique post-normalisation ────────────────────────────────
  // Les sources qui partagent series-35 ou divertissement contiennent tout → on filtre
  if (theme === 'thriller') {
    rtbfItems = rtbfItems.filter(i => i.theme === 'thriller');
    tf1Items  = tf1Items.filter(i => i.theme === 'thriller');
  } else if (theme === 'episodes') {
    rtbfItems = rtbfItems.filter(i => i.theme === 'episodes');
    tf1Items  = tf1Items.filter(i => i.theme === 'episodes' || i.theme === 'series');
  } else if (theme === 'telerealite') {
    rtbfItems = rtbfItems.filter(i => i.theme === 'telerealite');
    tf1Items  = tf1Items.filter(i => i.theme === 'telerealite');
  }

  rtbfItems = deduplicate(rtbfItems);
  tf1Items  = deduplicate(tf1Items);

  // ── Interleave RTBF + TF1 ─────────────────────────────────────────────────
  const merged: NormalizedItem[] = [];
  let r = 0, t = 0;
  while (r < rtbfItems.length || t < tf1Items.length) {
    if (r < rtbfItems.length) merged.push(rtbfItems[r++]);
    if (t < tf1Items.length)  merged.push(tf1Items[t++]);
  }

  console.log(`[/list] theme=${theme} rtbf=${rtbfItems.length} tf1=${tf1Items.length} total=${merged.length}`);

  return new Response(JSON.stringify({
    theme, label: THEMES[theme].label, emoji: THEMES[theme].emoji,
    items: merged,
    meta: { rtbf: rtbfItems.length, tf1: tf1Items.length, total: merged.length },
  }), {
    headers: { ...cors, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=300' },
  });
}

// ─── Handler principal ────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const cors = {
      'Access-Control-Allow-Origin':  '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });

    // ── /list ──────────────────────────────────────────────────────────────
    if (url.pathname === '/list') {
      try { return await handleListRequest(url, env, cors); }
      catch (err: any) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...cors, 'Content-Type': 'application/json' },
        });
      }
    }

    // ── /home ──────────────────────────────────────────────────────────────
    if (url.pathname !== '/home') {
      return new Response('Not found', { status: 404, headers: cors });
    }

    try {
      const [rtbfResult, tf1Result] = await Promise.allSettled([fetchRTBF(), fetchTF1()]);
      const rtbfHome = rtbfResult.status === 'fulfilled' ? rtbfResult.value : null;
      const tf1Raw   = tf1Result.status  === 'fulfilled' ? tf1Result.value  : null;

      if (rtbfResult.status === 'rejected') console.error('[/home] RTBF:', rtbfResult.reason);
      if (tf1Result.status  === 'rejected') console.error('[/home] TF1:',  tf1Result.reason);

      // Promobox RTBF
      let rtbfPromoItems: any[] = [];
      if (rtbfHome) {
        const promoWidget = (rtbfHome.data?.widgets ?? []).find((w: any) => w.type === 'PROMOBOX');
        if (promoWidget?.contentPath) {
          try {
            const pUrl = promoWidget.contentPath.startsWith('http')
              ? promoWidget.contentPath
              : `https://bff-service.rtbf.be${promoWidget.contentPath}`;
            const pRes = await fetch(pUrl, { headers: { Accept: 'application/json' } });
            if (pRes.ok) { const pj: any = await pRes.json(); rtbfPromoItems = pj?.data?.content ?? pj?.data ?? []; }
          } catch { /* silent */ }
        }
      }

      const heroBanners = [
        ...(rtbfHome ? buildRTBFBanners(rtbfHome, rtbfPromoItems) : []),
        ...(tf1Raw   ? buildTF1Banners(tf1Raw)                    : []),
      ];

      const llmCacheRaw = await env.LABEL_CACHE.get('label_map').catch(() => null);
      const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

      // Widgets RTBF home (1 page chacun — suffisant pour la home)
      let rtbfItems: NormalizedItem[] = [];
      if (rtbfHome) {
        const EXCL = new Set([
          'FAVORITE_PROGRAM_LIST', 'CHANNEL_LIST', 'ONGOING_PLAY_HISTORY',
          'CATEGORY_LIST', 'BANNER', 'MEDIA_TRAILER', 'PROMOBOX',
        ]);
        const wMetas = (rtbfHome.data?.widgets ?? [])
          .filter((w: any) => !EXCL.has(w.type) && w.contentPath)
          .map((w: any) => ({ title: w.title ?? '', fetch: fetchRTBFWidgetAll(w.contentPath, 1) }));

        const res = await Promise.allSettled(wMetas.map((m: any) => m.fetch));
        for (let i = 0; i < res.length; i++) {
          if (res[i].status !== 'fulfilled') continue;
          for (const raw of (res[i] as PromiseFulfilledResult<any[]>).value) {
            const item = normalizeRTBFItem(raw, llmCache, wMetas[i].title);
            if (item) rtbfItems.push(item);
          }
        }
      }

      // homeSliders TF1
      let tf1Items: NormalizedItem[] = [];
      if (tf1Raw) {
        for (const slider of tf1Raw.data?.homeSliders ?? []) {
          for (const item of slider.items ?? []) {
            const n = normalizeTF1Item(item, llmCache);
            if (n) tf1Items.push(n);
          }
        }
      }

      rtbfItems = deduplicate(rtbfItems);
      tf1Items  = deduplicate(tf1Items);

      // AI labels inconnus
      const allForAI = [...rtbfItems, ...tf1Items];
      const unknownLabels = [...new Set(
        allForAI
          .filter(i => i.theme === 'series' && i.categoryLabel)
          .map(i => normalizeLabel(i.categoryLabel!))
          .filter(l => !CATEGORY_MAP[l] && !llmCache[l]),
      )];

      let newMappings: Record<string, ThemeKey> = {};
      if (unknownLabels.length > 0) {
        newMappings = await classifyWithWorkersAI(unknownLabels, env);
        if (Object.keys(newMappings).length > 0) {
          await env.LABEL_CACHE.put(
            'label_map',
            JSON.stringify({ ...llmCache, ...newMappings }),
            { expirationTtl: 604800 },
          );
          for (const item of allForAI) {
            if (item.theme === 'series' && item.categoryLabel) {
              const k = normalizeLabel(item.categoryLabel);
              if (newMappings[k]) item.theme = newMappings[k];
            }
          }
          rtbfItems = allForAI.filter(i => i.platform === 'RTBF');
          tf1Items  = allForAI.filter(i => i.platform === 'TF1+');
        }
      }

      const buckets = buildBuckets(rtbfItems, tf1Items);
      console.log(`[/home] rtbf=${rtbfItems.length} tf1=${tf1Items.length} buckets=${buckets.length}`);

      return new Response(JSON.stringify({
        buckets, heroBanners,
        meta: {
          rtbf: rtbfItems.length, tf1: tf1Items.length,
          buckets: buckets.length,
          unknownLabelsClassifiedByAI: Object.keys(newMappings),
          totalUnknownLabels: unknownLabels.length,
        },
      }), {
        headers: { ...cors, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=300' },
      });

    } catch (err: any) {
      console.error('[/home] fatal:', err);
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500, headers: { ...cors, 'Content-Type': 'application/json' },
      });
    }
  },
};
