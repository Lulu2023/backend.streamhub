

export interface Env {
  AI: Ai;
  LABEL_CACHE: KVNamespace;
  DATA_CACHE:  KVNamespace;
}

const CACHE_TTL_STALE = 24 * 60 * 60; // 24h sécurité (cron toutes les 3h)

// ─── Types ────────────────────────────────────────────────────────────────────

type ThemeKey =
  | 'trending' | 'episodes' | 'thriller' | 'films' | 'series'
  | 'telerealite' | 'documentaire' | 'culture' | 'info' | 'sport' | 'kids' | 'top';

interface NormalizedItem {
  id:           string;
  title:        string;
  description?: string;
  illustration?: Record<string, string>;
  duration?:    number;
  platform:     'RTBF' | 'TF1+' | 'RTLplay';
  path?:        string;
  type:         string;
  theme:        ThemeKey;
  genres:       string[];
  labels?:      string[];
  _raw:         any;
}

interface ThematicBucket {
  theme:   ThemeKey;
  label:   string;
  emoji:   string;
  items:   NormalizedItem[];
  hasMore: boolean;
}

// ─── Constantes thèmes ────────────────────────────────────────────────────────

const THEMES: Record<ThemeKey, { label: string; emoji: string }> = {
  trending:     { label: 'En ce moment',         emoji: '🔥' },
  top:          { label: 'À ne pas manquer',      emoji: '⭐' },
  episodes:     { label: 'Épisodes récents',       emoji: '🎞️' },
  thriller:     { label: 'Policier & Thriller',    emoji: '🔍' },
  films:        { label: 'Films',                  emoji: '🎬' },
  series:       { label: 'Séries',                 emoji: '📺' },
  telerealite:  { label: 'Téléréalité',            emoji: '🎭' },
  documentaire: { label: 'Documentaires',          emoji: '📽️' },
  culture:      { label: 'Culture & Divertissement', emoji: '🎨' },
  info:         { label: 'Info & Actualités',      emoji: '📰' },
  sport:        { label: 'Sport',                  emoji: '⚽' },
  kids:         { label: 'Kids',                   emoji: '🌟' },
};

const BUCKET_ORDER: ThemeKey[] = [
  'trending', 'top', 'episodes', 'series', 'films', 'thriller',
  'telerealite', 'documentaire', 'culture', 'info', 'sport', 'kids',
];

const THEMES_WITH_LIST = new Set<ThemeKey>([
  'films', 'series', 'documentaire', 'culture', 'info', 'sport',
  'kids', 'telerealite', 'thriller', 'episodes',
]);

// ─── Keyword maps thèmes ──────────────────────────────────────────────────────

const CATEGORY_MAP: Record<string, ThemeKey> = {
  // Thriller / policier
  'policier': 'thriller', 'thriller': 'thriller', 'polar': 'thriller',
  'crime': 'thriller', 'suspense': 'thriller', 'espionnage': 'thriller',
  'affaires criminelles': 'thriller', 'serie policiere': 'thriller',
  // Films
  'film': 'films', 'films': 'films', 'cinema': 'films', 'telefilm': 'films',
  'biopic': 'films', 'western': 'films', 'comedie': 'films',
  'comedie dramatique': 'films', 'comedie romantique': 'films',
  'action': 'films', 'aventure': 'films', 'science-fiction': 'films',
  'fantastique': 'films', 'romance': 'films', 'drame': 'films',
  // Documentaires
  'documentaire': 'documentaire', 'reportage': 'documentaire',
  'investigation': 'documentaire', 'nature': 'documentaire',
  'science': 'documentaire', 'histoire': 'documentaire',
  'societe': 'documentaire', 'voyage': 'documentaire',
  'enquete': 'documentaire', 'environnement': 'documentaire',
  // Culture
  'culture': 'culture', 'humour': 'culture', 'musique': 'culture',
  'talk show': 'culture', 'varietes': 'culture', 'magazine': 'culture',
  'spectacle': 'culture', 'concert': 'culture', 'divertissement': 'culture',
  'game show': 'culture', 'quiz': 'culture', 'jeux': 'culture',
  'danse': 'culture', 'lifestyle': 'culture',
  // Info
  'info': 'info', 'actualite': 'info', 'journal': 'info',
  'politique': 'info', 'economie': 'info', 'debat': 'info',
  // Téléréalité
  'telerealite': 'telerealite', 'docu-realite': 'telerealite',
  'survie': 'telerealite', 'mariage': 'telerealite',
  'aventure / survie': 'telerealite',
  // Sport
  'sport': 'sport', 'football': 'sport', 'rugby': 'sport',
  'cyclisme': 'sport', 'tennis': 'sport', 'natation': 'sport',
  'athletisme': 'sport', 'formule 1': 'sport', 'basket': 'sport',
  // Kids
  'kids': 'kids', 'enfants': 'kids', 'jeunesse': 'kids',
  'anime': 'kids', 'dessin anime': 'kids',
  // Séries
  'serie': 'series', 'sitcom': 'series', 'feuilleton': 'series',
};

const GENRE_MAP: Record<string, string> = {
  'film': 'Film', 'telefilm': 'Téléfilm', 'biopic': 'Biopic',
  'comedie': 'Comédie', 'comedie dramatique': 'Comédie dramatique',
  'drame': 'Drame', 'romance': 'Romance', 'action': 'Action',
  'aventure': 'Aventure', 'science-fiction': 'Science-fiction',
  'fantastique': 'Fantastique', 'horreur': 'Horreur', 'western': 'Western',
  'policier': 'Policier', 'thriller': 'Thriller', 'crime': 'Crime',
  'espionnage': 'Espionnage', 'suspense': 'Thriller',
  'documentaire': 'Documentaire', 'reportage': 'Reportage',
  'investigation': 'Investigation', 'nature': 'Nature',
  'histoire': 'Histoire', 'societe': 'Société', 'voyage': 'Voyage',
  'humour': 'Humour', 'musique': 'Musique', 'concert': 'Concert',
  'spectacle': 'Spectacle', 'varietes': 'Variétés', 'talk show': 'Talk-show',
  'quiz': 'Jeux', 'jeux': 'Jeux', 'danse': 'Danse', 'people': 'People',
  'lifestyle': 'Lifestyle', 'divertissement': 'Divertissement',
  'actualite': 'Actualité', 'info': 'Info', 'journal': 'Journal',
  'politique': 'Politique', 'economie': 'Économie',
  'telerealite': 'Téléréalité', 'docu-realite': 'Docu-réalité',
  'survie': 'Survie', 'mariage': 'Mariage', 'famille': 'Famille',
  'sport': 'Sport', 'football': 'Football', 'rugby': 'Rugby',
  'cyclisme': 'Cyclisme', 'tennis': 'Tennis', 'athletisme': 'Athlétisme',
  'formule 1': 'Formule 1', 'basket': 'Basket',
  'jeunesse': 'Jeunesse', 'kids': 'Jeunesse', 'anime': 'Animé',
  'dessin anime': 'Animé', 'serie': 'Série', 'sitcom': 'Sitcom',
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

function nl(s: string): string {
  return s.toLowerCase().trim().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/\s+/g, ' ');
}

function resolveTheme(
  categoryLabel: string | undefined,
  topics: string[] | undefined,
  typology: string | undefined,
  llmCache: Record<string, ThemeKey>,
): ThemeKey {
  const check = (raw: string): ThemeKey | null => {
    const k = nl(raw.trim());
    if (CATEGORY_MAP[k]) return CATEGORY_MAP[k];
    for (const [f, t] of Object.entries(CATEGORY_MAP))
      if (f.length >= 5 && k.includes(f)) return t;
    return llmCache[k] ?? null;
  };
  if (typology) { const r = check(typology); if (r) return r; }
  if (categoryLabel) { const r = check(categoryLabel); if (r) return r; }
  for (const t of topics ?? []) { const r = check(t); if (r) return r; }
  return 'series';
}

function buildGenres(labels: (string | undefined)[]): string[] {
  const out = new Set<string>();
  for (const raw of labels) {
    if (!raw) continue;
    const k = nl(raw);
    if (GENRE_MAP[k]) { out.add(GENRE_MAP[k]); continue; }
    for (const [f, g] of Object.entries(GENRE_MAP))
      if (f.length >= 5 && k.includes(f)) { out.add(g); break; }
  }
  return [...out];
}

// ─── RTL Play ─────────────────────────────────────────────────────────────────

const RTL_ROW_THEME: Record<string, ThemeKey> = {
  'serie': 'series', 'series': 'series',
  'film': 'films', 'films': 'films',
  'telerealite': 'telerealite', 'realite': 'telerealite',
  'documentaire': 'documentaire', 'doc': 'documentaire',
  'sport': 'sport',
  'kids': 'kids', 'enfant': 'kids', 'jeunesse': 'kids',
  'info': 'info', 'actu': 'info',
  'culture': 'culture', 'humour': 'culture',
  'thriller': 'thriller', 'policier': 'thriller',
};

function rtlRowToTheme(rowTitle: string): ThemeKey {
  const t = nl(rowTitle);
  for (const [kw, theme] of Object.entries(RTL_ROW_THEME))
    if (t.includes(kw)) return theme;
  return 'trending';
}

async function fetchRTLHome(): Promise<any> {
  const res = await fetch(
    'https://lfvp-api.dpgmedia.net/RTL_PLAY/storefronts/accueil?itemsPerSwimlane=30&hideBannerRow=false',
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
  return res.json();
}

function normalizeRTLItem(teaser: any, rowTitle: string): NormalizedItem | null {
  if (!teaser?.title || !teaser?.imageUrl) return null;
  const theme     = rtlRowToTheme(rowTitle);
  const imageUrl  = teaser.imageUrl as string;
  const heroUrl   = teaser.heroImageUrl ?? imageUrl;
  // On construit illustration depuis imageUrl (landscape du CDN persgroep).
  // VideoCard affichera en portrait 2:3 via object-fit:cover grâce à isPortrait:true.
  const illustration = { xs: imageUrl, s: imageUrl, m: imageUrl, l: imageUrl, xl: heroUrl };

  return {
    id:          `rtl-${teaser.detailId ?? nl(teaser.title)}`,
    platform:    'RTLplay',
    title:       teaser.title,
    description: teaser.description ?? '',
    illustration,
    path:        teaser.detailId ? `/program/${teaser.detailId}` : '',
    type:        guessRTLType(teaser.labels),
    theme,
    genres:      buildGenres([rowTitle]),
    labels:      (teaser.labels ?? []).map((l: any) => l.label ?? l),
    _raw: {
      ...teaser,
      platform:     'RTLplay',
      illustration,
      isPortrait:   true,   // ← force portrait dans VideoCard (image landscape croppée 2:3)
    },
  };
}

function guessRTLType(labels: any[]): string {
  if (!labels) return 'SHOW';
  const s = labels.map((l: any) => (l.label ?? l) as string).join(' ').toLowerCase();
  if (/film|movie/.test(s)) return 'FILM';
  if (/saison|serie/.test(s)) return 'SERIE';
  return 'SHOW';
}

// ─── RTBF ─────────────────────────────────────────────────────────────────────

async function fetchRTBFHome(): Promise<any> {
  const res = await fetch(
    'https://bff-service.rtbf.be/auvio/v1.23/pages/home?userAgent=Chrome-web-3.0',
    { headers: { Accept: 'application/json' } },
  );
  if (!res.ok) throw new Error(`RTBF home ${res.status}`);
  return res.json();
}

/**
 * Fetch une page de widget RTBF et retourne les items + l'URL de la page suivante.
 * FIX : la réponse est { data: { content: [] } } — pas { data: [] }
 */
async function fetchRTBFPage(url: string): Promise<{ items: any[]; next: string | null }> {
  try {
    const full = url.startsWith('http') ? url : `https://bff-service.rtbf.be${url}`;
    const res  = await fetch(full, { headers: { Accept: 'application/json' } });
    if (!res.ok) return { items: [], next: null };
    const json: any = await res.json();

    // ← FIX : data.content (widget) ou data (direct array)
    const data  = json?.data;
    const items: any[] = Array.isArray(data?.content) ? data.content
                       : Array.isArray(data)           ? data
                       : [];

    const rawNext: string | null = json?.links?.next ?? null;
    const next = rawNext
      ? (rawNext.startsWith('http') ? rawNext : `https://bff-service.rtbf.be${rawNext}`)
      : null;
    return { items, next };
  } catch { return { items: [], next: null }; }
}

async function fetchRTBFWidgetAll(contentPath: string, maxPages = 4): Promise<any[]> {
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

async function fetchRTBFCategoryAll(categoryPath: string): Promise<{ items: any[]; widgetTitle: string }[]> {
  const res = await fetch(
    `https://bff-service.rtbf.be/auvio/v1.23/pages/categorie/${categoryPath}?userAgent=Chrome-web-3.0`,
    { headers: { Accept: 'application/json' } },
  );
  if (!res.ok) return [];
  const json: any = await res.json();
  const EXCL = new Set([
    'FAVORITE_PROGRAM_LIST', 'CHANNEL_LIST', 'ONGOING_PLAY_HISTORY',
    'CATEGORY_LIST', 'BANNER', 'MEDIA_TRAILER', 'PROMOBOX',
  ]);
  const widgets = (json?.data?.widgets ?? []).filter((w: any) => !EXCL.has(w.type) && w.contentPath);
  const results = await Promise.allSettled(
    widgets.map((w: any) =>
      fetchRTBFWidgetAll(w.contentPath, 5).then(items => ({ items, widgetTitle: w.title ?? '' })),
    ),
  );
  return results
    .filter((r): r is PromiseFulfilledResult<{ items: any[]; widgetTitle: string }> => r.status === 'fulfilled')
    .map(r => r.value);
}

function normalizeRTBFItem(
  item: any,
  llmCache: Record<string, ThemeKey>,
  widgetTitle = '',
): NormalizedItem | null {
  if (!item || item.resourceType === 'LIVE') return null;
  const illu = item.illustration;
  if (!illu && !item.title) return null;

  const isKids   = /kids|enfant|jeunesse/i.test(widgetTitle);
  const isSooner = item.resourceType === 'MEDIA_PREMIUM'
    || (Array.isArray(item.products) && item.products.some((p: any) => p.label === 'Sooner'));

  let theme: ThemeKey;
  if (isSooner)    theme = 'films';
  else if (isKids) theme = 'kids';
  else theme = resolveTheme(item.categoryLabel, undefined, undefined, llmCache);

  const isEpisode = item.resourceType === 'MEDIA' || item.type === 'VIDEO';
  if (isEpisode && ['series', 'films', 'thriller', 'telerealite'].includes(theme))
    theme = 'episodes';

  const illustration = illu ? {
    xs: illu.xs ?? '', s: illu.s ?? illu.xs ?? '',
    m: illu.m ?? illu.s ?? '', l: illu.l ?? illu.m ?? '', xl: illu.xl ?? illu.l ?? '',
  } : undefined;

  return {
    id:          `rtbf-${item.id ?? item.assetId}`,
    platform:    'RTBF',
    title:       item.title ?? '',
    description: item.description,
    illustration,
    duration:    item.duration,
    path:        item.path,
    type:        item.type ?? item.resourceType ?? 'SHOW',
    theme,
    genres:      buildGenres([item.categoryLabel, widgetTitle]),
    _raw:        item,
  };
}

// ─── TF1+ ─────────────────────────────────────────────────────────────────────

// Query ID capturé depuis les network requests TF1 (categoryBySlug avec sliders)
const TF1_CATEGORY_QID = '46f87e88577a61abb1d2a36a715a12d4175caa3d';
const TF1_HOME_QID      = 'c34093152db844db6b7ad9b56df12841f7d13182';
const TF1_BANNER_QID    = 'bd8e6aab9996844dad4ea9a53887adad27d86151';
const TF1_BASE          = 'https://www.tf1.fr/graphql/fr-be/web';
const TF1_HEADERS = {
  'content-type': 'application/json',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Origin': 'https://www.tf1.fr',
  'Referer': 'https://www.tf1.fr/',
};

const TF1_LIST_CONFIG: Partial<Record<ThemeKey, { slugs: string[] }>> = {
  films:        { slugs: ['films', 'telefilms'] },
  series:       { slugs: ['series'] },
  documentaire: { slugs: ['reportages'] },
  telerealite:  { slugs: ['divertissement'] },
  culture:      { slugs: ['divertissement'] },
  thriller:     { slugs: ['series'] },
  episodes:     { slugs: ['series'] },
  info:         { slugs: ['info'] },
  sport:        { slugs: ['sport'] },
  kids:         { slugs: ['jeunesse'] },
};

const RTBF_LIST_CONFIG: Partial<Record<ThemeKey, {
  type: 'category'; path: string;
} | { type: 'widgets'; ids: string[]; }>> = {
  films:        { type: 'category', path: 'films-36' },
  series:       { type: 'category', path: 'series-35' },
  documentaire: { type: 'category', path: 'documentaires-31' },
  info:         { type: 'category', path: 'info-1' },
  sport:        { type: 'category', path: 'sport-9' },
  kids:         { type: 'widgets', ids: ['22390'] },
  telerealite:  { type: 'category', path: 'series-35' },
  thriller:     { type: 'category', path: 'series-35' },
  episodes:     { type: 'category', path: 'series-35' },
  culture:      { type: 'widgets', ids: ['20136', '20691'] },
};

async function fetchTF1CategorySliders(slugs: string[]): Promise<any[]> {
  const all: any[] = [];
  for (const slug of slugs) {
    const variables = encodeURIComponent(JSON.stringify({
      categorySlug: slug, limit: 50,
      ofContentTypes: ['PROGRAM', 'VIDEO', 'TOP_PROGRAM', 'TOP_VIDEO', 'ARTICLE'],
      ofBannerTypes: ['LARGE', 'MEDIUM'],
      ofChannelTypes: ['CORNER', 'DIGITAL', 'EVENT', 'PARTNER', 'TV'],
    }));
    try {
      const res = await fetch(`${TF1_BASE}?id=${TF1_CATEGORY_QID}&variables=${variables}`, {
        method: 'GET', headers: TF1_HEADERS,
      });
      if (!res.ok) { console.error(`[TF1 cat] ${res.status} slug=${slug}`); continue; }
      const json: any = await res.json();
      const cat = json?.data?.categoryBySlug ?? {};
      for (const cover of (cat.covers ?? []))
        if (cover.__typename !== 'CoverOfExternalLink') all.push(cover);
      for (const slider of (cat.sliders ?? []))
        for (const item of (slider.items ?? []))
          all.push({ ...item, _sliderTitle: slider.title ?? '' });
    } catch (err) { console.error(`[TF1 cat] slug=${slug}:`, err); }
  }
  return all;
}

async function fetchTF1Home(): Promise<any> {
  const homeVars = encodeURIComponent(JSON.stringify({
    ofBannerTypes: ['LARGE', 'MEDIUM'],
    ofContentTypes: ['PROGRAM', 'VIDEO', 'TOP_PROGRAM', 'TOP_VIDEO'],
    ofChannelTypes: ['CORNER', 'DIGITAL', 'EVENT', 'PARTNER', 'TV'],
  }));
  const [homeRes, bannerRes] = await Promise.allSettled([
    fetch(`${TF1_BASE}?id=${TF1_HOME_QID}&variables=${homeVars}`, { headers: TF1_HEADERS }),
    fetch(`https://www.tf1.fr/graphql/web?id=${TF1_BANNER_QID}`, { headers: TF1_HEADERS }),
  ]);
  const homeJson   = homeRes.status   === 'fulfilled' && homeRes.value.ok   ? await homeRes.value.json()   : null;
  const bannerJson = bannerRes.status === 'fulfilled' && bannerRes.value.ok ? await bannerRes.value.json() : null;
  return { ...(homeJson ?? {}), _tf1Banners: bannerJson?.data?.homeCoversByRight ?? [] };
}

function pickBestUrl(sources: any[]): string {
  if (!sources?.length) return '';
  return ([...sources].sort((a, b) => (b.scale ?? 0) - (a.scale ?? 0))
    .find(s => s.type === 'webp' || s.type === 'jpg' || s.url))?.url ?? '';
}

function buildIllustrationFromSources(sources: any[]): Record<string, string> | undefined {
  if (!sources?.length) return undefined;
  const pool   = sources.filter(s => s.type === 'webp').length ? sources.filter(s => s.type === 'webp') : sources;
  const sorted = [...pool].sort((a, b) => (b.scale ?? 0) - (a.scale ?? 0));
  const best   = sorted[0]?.url;
  if (!best) return undefined;
  const s3 = sorted.find(s => s.scale === 3)?.url ?? best;
  const s2 = sorted.find(s => s.scale === 2)?.url ?? best;
  const s1 = sorted.find(s => s.scale === 1)?.url ?? best;
  return { xl: s3, l: s3, m: s2, s: s2, xs: s1 };
}

function normalizeTF1Item(item: any, llmCache: Record<string, ThemeKey>): NormalizedItem | null {
  if (!item) return null;
  const prog  = item.program ?? item;
  const id    = prog.id ?? item.id;
  const title = prog.decoration?.label ?? prog.name ?? item.decoration?.label ?? item.name ?? '';
  if (!title || !id) return null;

  const typology: string = prog.typology ?? item.typology ?? '';
  const topics: string[] = prog.topics   ?? item.program?.topics ?? [];
  const duration         = item.duration ?? prog.duration ?? 0;

  const theme = resolveTheme(typology || undefined, topics, typology || undefined, llmCache);

  // Images portrait (priorité) puis landscape en fallback
  const portraitSrc =
    item.decoration?.coverSmall?.sourcesWithScales ??
    prog.decoration?.portrait?.sourcesWithScales ??
    item.decoration?.portrait?.sourcesWithScales ??
    item.video?.program?.decoration?.portrait?.sourcesWithScales ??
    prog.decoration?.coverSmall?.sourcesWithScales;

  const landscapeSrc =
    prog.decoration?.thumbnail?.sourcesWithScales ??
    item.decoration?.thumbnail?.sourcesWithScales ??
    item.image?.sourcesWithScales;

  const illustration =
    buildIllustrationFromSources(portraitSrc ?? [])
    ?? buildIllustrationFromSources(landscapeSrc ?? []);

  const fallbackUrl = !illustration ? pickBestUrl([...(portraitSrc ?? []), ...(landscapeSrc ?? [])]) : '';
  const finalIllus  = illustration ?? (fallbackUrl ? { xs: fallbackUrl, s: fallbackUrl, m: fallbackUrl, l: fallbackUrl, xl: fallbackUrl } : undefined);

  const isFilm  = typology === 'Film' || typology === 'Téléfilm';
  const isVideo = item.__typename === 'Video';
  const resourceType = (isVideo || isFilm) ? 'MEDIA' : 'PROGRAM';

  const enrichedRaw = {
    ...item,
    id, title,
    description: prog.synopsis ?? prog.decoration?.description,
    illustration: finalIllus,
    duration, typology,
    slug:         prog.slug ?? item.slug,
    resourceType,
    platform:     'TF1+',
    isPortrait:   !!portraitSrc,
    ...(isFilm ? { isFilm: true } : {}),
  };

  return {
    id:          `tf1-${id}`,
    platform:    'TF1+',
    title,
    description: enrichedRaw.description,
    illustration: finalIllus,
    duration,
    path:        `/tf1/${resourceType === 'MEDIA' ? 'video' : 'program'}/${id}`,
    type:        resourceType,
    theme,
    genres:      buildGenres([item._sliderTitle, typology, ...(topics ?? [])]),
    _raw:        enrichedRaw,
  };
}

// ─── Banners ──────────────────────────────────────────────────────────────────

function buildRTLBanners(rtlRaw: any): any[] {
  const top = (rtlRaw?.rows ?? []).find((r: any) => r.rowType === 'TOP_BANNER');
  if (!top?.teaser) return [];
  const t = top.teaser;
  return [{
    id: `rtl-banner-${t.target?.id ?? 'top'}`,
    platform: 'RTLplay', title: t.title ?? '',
    description: t.byline ?? '', largeImageUrl: t.largeImageUrl ?? t.mediumImageUrl ?? '',
    detailId: t.target?.id ?? null,
  }];
}

function buildRTBFBanners(rtbfHome: any, promoItems: any[]): any[] {
  const banners: any[] = [];
  for (const d of promoItems) {
    if (!d.title) continue;
    const img = d.image
      ? { xs: d.image.xs ?? '', s: d.image.s ?? '', m: d.image.m ?? '', l: d.image.l ?? '', xl: d.image.xl ?? d.image.l ?? '' }
      : null;
    banners.push({ id: `rtbf-banner-${d.id ?? Math.random()}`, platform: 'RTBF', title: d.title, description: d.subtitle ?? '', image: img, deepLink: d.deeplink ?? null });
  }
  return banners;
}

function buildTF1Banners(tf1Raw: any): any[] {
  const banners: any[] = [];
  for (const cover of (tf1Raw?._tf1Banners ?? []).slice(0, 5)) {
    const deco = cover.decoration ?? {}, prog = cover.program ?? {};
    const id   = cover.id ?? prog.id;
    const title = deco.label ?? prog.name ?? '';
    if (!id || !title) continue;
    const bgS = [...(deco.cover?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 0) - (a.scale ?? 0));
    banners.push({
      id: `tf1-banner-${id}`, platform: 'TF1+', title,
      description: deco.description ?? deco.catchPhrase ?? '',
      largeImageUrl: bgS[0]?.url ?? '',
    });
  }
  return banners;
}

// ─── CF AI : classification thèmes ────────────────────────────────────────────

/**
 * AI utilisée UNIQUEMENT pour les labels catégorie inconnus du CATEGORY_MAP.
 * Résultat mis en cache 7j dans LABEL_CACHE["label_map"].
 * Exemple : "Comédie musicale" → 'culture', "Docu-série criminelle" → 'thriller'
 */
async function classifyWithWorkersAI(labels: string[], env: Env): Promise<Record<string, ThemeKey>> {
  if (!labels.length) return {};
  const resolved: Record<string, ThemeKey> = {};
  try {
    const res = await env.AI.run('@cf/meta/llama-3-8b-instruct' as any, {
      messages: [{
        role: 'user',
        content: `Classifie chaque label dans un thème parmi : ${Object.keys(THEMES).join(', ')}.
JSON uniquement. Format : {"label": "theme"}
${labels.map(l => `- "${l}"`).join('\n')}`,
      }],
      max_tokens: 512,
    } as any) as any;
    const text = (res?.response ?? '').replace(/```json|```/g, '');
    const start = text.indexOf('{'), end = text.lastIndexOf('}');
    if (start !== -1) {
      const parsed = JSON.parse(text.slice(start, end + 1)) as Record<string, string>;
      for (const [lbl, th] of Object.entries(parsed))
        if (th in THEMES) resolved[nl(lbl)] = th as ThemeKey;
    }
  } catch (err) {
    console.error('[AI themes]', err);
    for (const lbl of labels) resolved[nl(lbl)] = 'series';
  }
  return resolved;
}

/**
 * AI utilisée pour enrichir les genres d'affichage des items sans métadonnées structurées.
 * Exemple : item RTL sans categoryLabel → l'IA devine le genre depuis le titre.
 * Cache 7j dans LABEL_CACHE["genres_map"] (clé = itemId).
 */
async function classifyGenresWithAI(
  items: Array<{ id: string; title: string; description?: string }>,
  env: Env,
): Promise<Map<string, string[]>> {
  const result = new Map<string, string[]>();
  if (!items.length) return result;

  let cache: Record<string, string[]> = {};
  try { const r = await env.LABEL_CACHE.get('genres_map'); if (r) cache = JSON.parse(r); } catch {}

  const toClassify = items.filter(i => !cache[i.id]);
  if (!toClassify.length) {
    for (const i of items) if (cache[i.id]) result.set(i.id, cache[i.id]);
    return result;
  }

  const vocab  = [...new Set(Object.values(GENRE_MAP))].join(', ');
  const BATCH  = 20;
  for (let start = 0; start < toClassify.length; start += BATCH) {
    const batch = toClassify.slice(start, start + BATCH);
    const lines = batch.map((i, idx) => `${idx}: "${i.title}"${i.description ? ` — ${i.description.slice(0, 60)}` : ''}`).join('\n');
    try {
      const res = await env.AI.run('@cf/meta/llama-3-8b-instruct' as any, {
        messages: [{
          role: 'user',
          content: `Genres vidéo parmi : ${vocab}\nJSON uniquement : {"0":["Genre1"],"1":["Genre1","Genre2"]}\nItems :\n${lines}`,
        }],
        max_tokens: 512,
      } as any) as any;
      const text  = (res?.response ?? '').replace(/```json|```/g, '');
      const start2 = text.indexOf('{'), end2 = text.lastIndexOf('}');
      if (start2 !== -1) {
        const parsed = JSON.parse(text.slice(start2, end2 + 1)) as Record<string, string[]>;
        for (const [idx, genres] of Object.entries(parsed)) {
          const item = batch[parseInt(idx)];
          if (!item) continue;
          const valid = (Array.isArray(genres) ? genres : []).filter(g => Object.values(GENRE_MAP).includes(g));
          if (valid.length) { result.set(item.id, valid); cache[item.id] = valid; }
        }
      }
    } catch (err) { console.error('[AI genres] batch', err); }
  }
  try { await env.LABEL_CACHE.put('genres_map', JSON.stringify(cache), { expirationTtl: 604800 }); } catch {}
  for (const i of items) if (cache[i.id]) result.set(i.id, cache[i.id]);
  return result;
}

// ─── Déduplication ────────────────────────────────────────────────────────────

function deduplicate(items: NormalizedItem[]): NormalizedItem[] {
  const seen = new Map<string, NormalizedItem>();
  for (const item of items) {
    const key = `${item.platform}:${nl(item.title)}`;
    if (!seen.has(key)) seen.set(key, item);
  }
  return [...seen.values()];
}

// ─── Build buckets (interleave RTL+RTBF+TF1) ─────────────────────────────────

function buildBuckets(items: NormalizedItem[]): ThematicBucket[] {
  const byTheme = new Map<ThemeKey, NormalizedItem[]>();
  for (const th of BUCKET_ORDER) byTheme.set(th, []);
  for (const item of items) {
    const th = item.theme;
    if (!byTheme.has(th)) byTheme.set(th, []);
    byTheme.get(th)!.push(item);
  }

  return BUCKET_ORDER
    .map(theme => {
      const all  = byTheme.get(theme) ?? [];
      if (!all.length) return null;
      // Interleave RTL + RTBF + TF1
      const byP: Record<string, NormalizedItem[]> = {};
      for (const i of all) { if (!byP[i.platform]) byP[i.platform] = []; byP[i.platform].push(i); }
      const ps     = Object.keys(byP);
      const mixed: NormalizedItem[] = [];
      for (let i = 0; mixed.length < all.length; i++) {
        const p = ps[i % ps.length];
        if (byP[p]?.length) mixed.push(byP[p].shift()!);
        if (ps.every(p => !byP[p]?.length)) break;
      }
      return {
        theme, label: THEMES[theme].label, emoji: THEMES[theme].emoji,
        items: mixed.slice(0, 24),
        hasMore: THEMES_WITH_LIST.has(theme),
      };
    })
    .filter(Boolean) as ThematicBucket[];
}

// ─── Build home ───────────────────────────────────────────────────────────────

async function buildHomeData(env: Env) {
  const llmCacheRaw = await env.LABEL_CACHE.get('label_map').catch(() => null);
  const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

  const [rtlRes, rtbfRes, tf1Res] = await Promise.allSettled([
    fetchRTLHome(),
    fetchRTBFHome(),
    fetchTF1Home(),
  ]);

  const rtlRaw  = rtlRes.status  === 'fulfilled' ? rtlRes.value  : null;
  const rtbfRaw = rtbfRes.status === 'fulfilled' ? rtbfRes.value : null;
  const tf1Raw  = tf1Res.status  === 'fulfilled' ? tf1Res.value  : null;

  // ── RTL items ──
  const rtlItems: NormalizedItem[] = [];
  for (const row of (rtlRaw?.rows ?? [])) {
    if (!row.teasers) continue;
    for (const t of row.teasers) {
      const item = normalizeRTLItem(t, row.title ?? '');
      if (item) rtlItems.push(item);
    }
  }

  // ── RTBF items (depuis widgets home) ──
  let rtbfItems: NormalizedItem[] = [];
  if (rtbfRaw) {
    const EXCL = new Set(['FAVORITE_PROGRAM_LIST','CHANNEL_LIST','ONGOING_PLAY_HISTORY','CATEGORY_LIST','BANNER','MEDIA_TRAILER','PROMOBOX','MEDIA_PREMIUM_LIST']);
    const widgets = (rtbfRaw.data?.widgets ?? []).filter((w: any) => !EXCL.has(w.type) && w.contentPath);
    const results = await Promise.allSettled(
      widgets.map((w: any) => fetchRTBFWidgetAll(w.contentPath, 1).then(items => ({ items, title: w.title ?? '' }))),
    );
    for (const r of results) {
      if (r.status !== 'fulfilled') continue;
      for (const raw of r.value.items) {
        const item = normalizeRTBFItem(raw, llmCache, r.value.title);
        if (item) rtbfItems.push(item);
      }
    }
    rtbfItems = deduplicate(rtbfItems);
  }

  // ── TF1 items ──
  let tf1Items: NormalizedItem[] = [];
  if (tf1Raw) {
    for (const slider of (tf1Raw.data?.homeSliders ?? [])) {
      for (const item of (slider.items ?? [])) {
        const n = normalizeTF1Item(item, llmCache);
        if (n) tf1Items.push(n);
      }
    }
    tf1Items = deduplicate(tf1Items);
  }

  // ── AI : labels inconnus → thèmes ──
  const allItems = [...rtlItems, ...rtbfItems, ...tf1Items];
  const unknown  = [...new Set(
    allItems.filter(i => i.theme === 'series' && (i._raw?.categoryLabel || i._raw?.typology))
      .map(i => nl(i._raw?.categoryLabel ?? i._raw?.typology ?? ''))
      .filter(l => l && !CATEGORY_MAP[l] && !llmCache[l]),
  )];
  if (unknown.length > 0) {
    const mapped = await classifyWithWorkersAI(unknown, env);
    if (Object.keys(mapped).length) {
      await env.LABEL_CACHE.put('label_map', JSON.stringify({ ...llmCache, ...mapped }), { expirationTtl: 604800 }).catch(() => {});
      for (const item of allItems) {
        const lbl = nl(item._raw?.categoryLabel ?? item._raw?.typology ?? '');
        if (lbl && mapped[lbl]) item.theme = mapped[lbl];
      }
    }
  }

  // ── Banners ──
  let rtbfPromoItems: any[] = [];
  if (rtbfRaw) {
    const promoW = (rtbfRaw.data?.widgets ?? []).find((w: any) => w.type === 'PROMOBOX');
    if (promoW?.contentPath) {
      try {
        const pr = await fetch(promoW.contentPath.startsWith('http') ? promoW.contentPath : `https://bff-service.rtbf.be${promoW.contentPath}`, { headers: { Accept: 'application/json' } });
        if (pr.ok) { const pj: any = await pr.json(); rtbfPromoItems = pj?.data?.content ?? pj?.data ?? []; }
      } catch {}
    }
  }

  const heroBanners = [
    ...(rtlRaw  ? buildRTLBanners(rtlRaw)                    : []),
    ...(rtbfRaw ? buildRTBFBanners(rtbfRaw, rtbfPromoItems)  : []),
    ...(tf1Raw  ? buildTF1Banners(tf1Raw)                    : []),
  ];

  const buckets = buildBuckets(allItems);
  console.log(`[home] rtl=${rtlItems.length} rtbf=${rtbfItems.length} tf1=${tf1Items.length} buckets=${buckets.length}`);

  return {
    buckets, heroBanners,
    meta: {
      rtl: rtlItems.length, rtbf: rtbfItems.length, tf1: tf1Items.length,
      total: allItems.length, buckets: buckets.length,
      builtAt: Date.now(),
    },
  };
}

// ─── Build /list ───────────────────────────────────────────────────────────────

async function buildListData(theme: ThemeKey, env: Env) {
  const llmCacheRaw = await env.LABEL_CACHE.get('label_map').catch(() => null);
  const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

  const rtbfCfg = RTBF_LIST_CONFIG[theme];
  const tf1Cfg  = TF1_LIST_CONFIG[theme];

  const rtbfPromise = rtbfCfg
    ? rtbfCfg.type === 'category'
      ? fetchRTBFCategoryAll(rtbfCfg.path)
      : Promise.all(rtbfCfg.ids.map(id =>
          fetchRTBFWidgetAll(`https://bff-service.rtbf.be/auvio/v1.23/widgets/${id}`, 6)
            .then(items => ({ items, widgetTitle: '' })),
        ))
    : Promise.resolve([]);

  const tf1Promise = tf1Cfg ? fetchTF1CategorySliders(tf1Cfg.slugs) : Promise.resolve([]);

  const [rtbfRes, tf1Res] = await Promise.allSettled([rtbfPromise, tf1Promise]);

  let rtbfItems: NormalizedItem[] = [];
  if (rtbfRes.status === 'fulfilled')
    for (const { items, widgetTitle } of rtbfRes.value)
      for (const raw of items) {
        const item = normalizeRTBFItem(raw, llmCache, widgetTitle);
        if (item) rtbfItems.push(item);
      }

  let tf1Items: NormalizedItem[] = [];
  if (tf1Res.status === 'fulfilled')
    for (const raw of tf1Res.value) {
      const item = normalizeTF1Item(raw, llmCache);
      if (item) tf1Items.push(item);
    }

  // Filtre thématique
  const themeFilter: Partial<Record<ThemeKey, ThemeKey[]>> = {
    thriller:    ['thriller'],
    telerealite: ['telerealite'],
    episodes:    ['episodes', 'series'],
  };
  if (themeFilter[theme]) {
    const allowed = new Set(themeFilter[theme]);
    rtbfItems = rtbfItems.filter(i => allowed.has(i.theme));
    tf1Items  = tf1Items.filter(i => allowed.has(i.theme));
  }

  rtbfItems = deduplicate(rtbfItems);
  tf1Items  = deduplicate(tf1Items);

  // Enrichissement genres IA pour les items sans genres
  const allItems = [...rtbfItems, ...tf1Items];
  const needsAI  = allItems.filter(i => i.genres.length === 0);
  if (needsAI.length > 0) {
    const aiResult = await classifyGenresWithAI(
      needsAI.map(i => ({ id: i.id, title: i.title, description: i.description })),
      env,
    );
    for (const item of allItems)
      if (!item.genres.length) { const g = aiResult.get(item.id); if (g?.length) item.genres = g; }
  }

  // Interleave RTBF + TF1
  const merged: NormalizedItem[] = [];
  let r = 0, t = 0;
  while (r < rtbfItems.length || t < tf1Items.length) {
    if (r < rtbfItems.length) merged.push(rtbfItems[r++]);
    if (t < tf1Items.length)  merged.push(tf1Items[t++]);
  }

  console.log(`[list:${theme}] rtbf=${rtbfItems.length} tf1=${tf1Items.length} total=${merged.length}`);
  return {
    items: merged,
    meta: { rtbf: rtbfItems.length, tf1: tf1Items.length, total: merged.length, builtAt: Date.now() },
  };
}

// ─── Refresh cron ─────────────────────────────────────────────────────────────

async function refreshAll(env: Env): Promise<void> {
  console.log('[refresh] Start');
  try {
    const home = await buildHomeData(env);
    await env.DATA_CACHE.put('home_v3', JSON.stringify(home), { expirationTtl: CACHE_TTL_STALE });
    console.log('[refresh] home OK');
  } catch (err) { console.error('[refresh] home:', err); }

  const themes = [...THEMES_WITH_LIST] as ThemeKey[];
  for (let i = 0; i < themes.length; i += 3) {
    await Promise.allSettled(themes.slice(i, i + 3).map(async th => {
      try {
        const data = await buildListData(th, env);
        await env.DATA_CACHE.put(`list_v3_${th}`, JSON.stringify(data), { expirationTtl: CACHE_TTL_STALE });
        console.log(`[refresh] list:${th} OK (${data.meta.total})`);
      } catch (err) { console.error(`[refresh] list:${th}:`, err); }
    }));
  }
  console.log('[refresh] Done');
}

// ─── Entry point ──────────────────────────────────────────────────────────────

export default {
  async scheduled(_: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(refreshAll(env));
  },

  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const cors = {
      'Access-Control-Allow-Origin':  '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });

    const { pathname, searchParams } = new URL(request.url);
    const json = (body: any, extra?: Record<string, string>) =>
      new Response(JSON.stringify(body), { headers: { ...cors, 'Content-Type': 'application/json', ...extra } });

    try {
      // ── /home ──────────────────────────────────────────────────────────────
      if (pathname === '/home') {
        let cached = await env.DATA_CACHE.get('home_v3').catch(() => null);
        if (!cached || searchParams.has('refresh')) {
          const data = await buildHomeData(env);
          cached     = JSON.stringify(data);
          ctx.waitUntil(env.DATA_CACHE.put('home_v3', cached, { expirationTtl: CACHE_TTL_STALE }));
        }
        return new Response(cached, { headers: { ...cors, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=300' } });
      }

      // ── /list?theme=X&page=N ───────────────────────────────────────────────
      if (pathname === '/list') {
        const theme = searchParams.get('theme') as ThemeKey | null;
        if (!theme || !(theme in THEMES)) return json({ error: 'theme invalide' }, undefined);

        let cached = await env.DATA_CACHE.get(`list_v3_${theme}`).catch(() => null);
        if (!cached) {
          const data = await buildListData(theme, env);
          cached     = JSON.stringify(data);
          ctx.waitUntil(env.DATA_CACHE.put(`list_v3_${theme}`, cached, { expirationTtl: CACHE_TTL_STALE }));
        }
        const full     = JSON.parse(cached) as { items: NormalizedItem[]; meta: any };
        const PAGE     = 48;
        const page     = Math.max(1, parseInt(searchParams.get('page') ?? '1'));
        const pageItems = full.items.slice((page - 1) * PAGE, page * PAGE);
        return json({
          theme, label: THEMES[theme].label, emoji: THEMES[theme].emoji, items: pageItems,
          meta: { ...full.meta, page, pageSize: PAGE, totalPages: Math.ceil(full.items.length / PAGE), hasMore: page * PAGE < full.items.length },
        }, { 'Cache-Control': 'public, max-age=300' });
      }

      // ── /genres?theme=X ────────────────────────────────────────────────────
      if (pathname === '/genres') {
        const theme = searchParams.get('theme') as ThemeKey | null;
        if (!theme || !(theme in THEMES)) return json({ error: 'theme invalide' }, undefined);
        const cached = await env.DATA_CACHE.get(`list_v3_${theme}`).catch(() => null);
        if (!cached) return json({ genres: [] });
        const data   = JSON.parse(cached) as { items: NormalizedItem[] };
        return json({ genres: data.items.map(i => ({ id: i.id, genres: i.genres })) },
          { 'Cache-Control': 'public, max-age=600' });
      }

      return json({ endpoints: ['/home', '/home?refresh', '/list?theme=X&page=N', '/genres?theme=X'] });

    } catch (err: any) {
      console.error('[Worker]', err);
      return new Response(JSON.stringify({ error: String(err) }), { status: 500, headers: { ...cors, 'Content-Type': 'application/json' } });
    }
  },
};
