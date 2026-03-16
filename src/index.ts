/**
 * Cloudflare Worker — Aggregateur multi-plateforme StreamHub
 *
 * GET /home → { buckets, heroBanners, meta }
 * GET /list?theme=<themeKey> → { theme, label, emoji, items, meta }
 *
 * Bindings requis (wrangler.toml) :
 *   [ai]            binding = "AI"
 *   [[kv_namespaces]] binding = "LABEL_CACHE"
 */

export interface Env {
  AI: Ai;
  LABEL_CACHE: KVNamespace;
}

// ─── Types ────────────────────────────────────────────────────────────────────

type ThemeKey =
  | 'top'          // ⭐ Top 10 TF1 (TopProgramItem)
  | 'sooner'       // 🎬 Sooner — films premium RTBF (MEDIA_PREMIUM_LIST)
  | 'episodes'     // 🎞️ Épisodes récents RTBF (VIDEO en cours de diffusion)
  | 'thriller'
  | 'films'
  | 'series'
  | 'documentaire'
  | 'culture'
  | 'info'
  | 'sport'
  | 'kids'
  | 'telerealite';

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
  hasMore: boolean; // indique si un /list?theme=X est disponible
}

// ─── Map de classification statique ──────────────────────────────────────────

const CATEGORY_MAP: Record<string, ThemeKey> = {
  // ── Policier / Thriller ──────────────────────────────────────────────────
  'policier': 'thriller', 'affaires criminelles': 'thriller', 'crime': 'thriller',
  'thriller': 'thriller', 'serie policiere': 'thriller', 'série policière': 'thriller',
  'police': 'thriller', 'suspense': 'thriller', 'horreur': 'thriller',
  'espionnage': 'thriller', 'polar': 'thriller',

  // ── Films ────────────────────────────────────────────────────────────────
  'film': 'films', 'films': 'films',
  'comédie': 'films', 'comedie': 'films',
  'comédie dramatique': 'films', 'comedie dramatique': 'films',
  'action': 'films', 'aventure': 'films',
  'science-fiction': 'films', 'sf': 'films', 'fantastique': 'films',
  'animation': 'films', 'romance': 'films', 'western': 'films',
  'biopic': 'films', 'téléfilm': 'films', 'telefilm': 'films',
  'drame': 'films', 'cinema': 'films', 'comédie romantique': 'films',

  // ── Documentaire ─────────────────────────────────────────────────────────
  'documentaire': 'documentaire', 'investigation': 'documentaire',
  'société': 'documentaire', 'societe': 'documentaire',
  'histoire': 'documentaire', 'découvertes': 'documentaire', 'decouvertes': 'documentaire',
  'reportage': 'documentaire', 'nature': 'documentaire', 'science': 'documentaire',
  'environnement': 'documentaire', 'voyage': 'documentaire',
  'monde': 'documentaire', 'enquête': 'documentaire',

  // ── Culture & Divertissement ──────────────────────────────────────────────
  'culture': 'culture', 'divertissement': 'culture', 'humour': 'culture',
  'musique': 'culture', 'talk show': 'culture', 'variétés': 'culture',
  'varietes': 'culture', 'magazine': 'culture', 'lifestyle': 'culture',
  'spectacle': 'culture', 'concert': 'culture',
  'people & musique': 'culture', 'people': 'culture',
  'litterature': 'culture', 'littérature': 'culture',
  'jeux': 'culture', 'jeux & divertissements': 'culture',
  'émission': 'culture', 'emission': 'culture',
  'émission de flux': 'culture', 'game show': 'culture', 'quiz': 'culture',
  'talk-show': 'culture', 'variété': 'culture',

  // ── Info & Actualités ─────────────────────────────────────────────────────
  'info': 'info', 'actualité': 'info', 'actualités': 'info', 'journal': 'info',
  'politique': 'info', 'économie': 'info', 'economie': 'info',
  'news': 'info', 'débat': 'info', 'debat': 'info',
  'information': 'info',
  'actualite': 'info', 'magazine d\'info': 'info',

  // ── Téléréalité ───────────────────────────────────────────────────────────
  'téléréalité': 'telerealite', 'telerealite': 'telerealite',
  'docu-réalité': 'telerealite', 'docu-realite': 'telerealite',
  'docu-réal': 'telerealite', 'docureality': 'telerealite',
  'real': 'telerealite', 'reality': 'telerealite',
  'aventure / survie': 'telerealite',

  // ── Kids ──────────────────────────────────────────────────────────────────
  'kids': 'kids', 'enfants': 'kids', 'jeunesse': 'kids',
  'animé': 'kids', 'anime': 'kids', 'dessin animé': 'kids',

  // ── Sport ─────────────────────────────────────────────────────────────────
  'sport': 'sport', 'football': 'sport', 'cyclisme': 'sport', 'tennis': 'sport',
  'rugby': 'sport', 'formule 1': 'sport', 'athlétisme': 'sport',
  'moteurs': 'sport', 'basket': 'sport', 'natation': 'sport',
  'f1': 'sport', 'moto': 'sport', 'golf': 'sport', 'boxe': 'sport',

  // ── Séries (fallback) ─────────────────────────────────────────────────────
  'serie': 'series', 'série': 'series', 'sitcom': 'series', 'feuilleton': 'series',
  'mini serie': 'series', 'mini série': 'series',
};

const TF1_TOPICS_MAP: Record<string, ThemeKey> = {
  'téléréalité': 'telerealite', 'docu-réalité': 'telerealite', 'survie': 'telerealite',
  'mariage': 'telerealite', 'famille': 'telerealite', 'lifestyle': 'telerealite',
  'danse': 'culture', 'chanson': 'culture', 'divertissement': 'culture',
  'musique': 'culture', 'concert': 'culture', 'spectacle': 'culture',
  'quiz': 'culture', 'humour': 'culture', 'culture': 'culture', 'talk show': 'culture',
  'sport': 'sport', 'football': 'sport', 'cyclisme': 'sport', 'rugby': 'sport',
  'athletisme': 'sport', 'docu-réalité sportive': 'sport',
  'actualité': 'info', 'journal télévisé': 'info', 'faits divers': 'info', 'societe': 'info',
  'reportages': 'documentaire', 'enquête': 'documentaire', 'nature': 'documentaire', 'histoire': 'documentaire',
  'policier': 'thriller', 'thriller': 'thriller', 'suspense': 'thriller', 'crime': 'thriller',
  'action': 'films', 'aventure': 'films', 'drame': 'films', 'comédie': 'films',
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

// ─── Mappings pour l'endpoint /list ──────────────────────────────────────────

// Chemins des pages catégories RTBF (ex: /categorie/films-36)
const RTBF_CATEGORY_PATHS: Partial<Record<ThemeKey, string>> = {
  films:        'films-36',
  series:       'series-35',
  documentaire: 'documentaires-31',
  info:         'info-1',
  sport:        'sport-9',
};

// IDs des widgets RTBF pour les thèmes sans page catégorie dédiée
const RTBF_WIDGET_IDS: Partial<Record<ThemeKey, string>> = {
  kids:     '22390',
  sooner:   '19737',
  culture:  '20136',
  episodes: '18601', // "Notre sélection" — recent media items
};

// Titres de widget à forcer pour la détection Kids
const RTBF_WIDGET_FORCE_TITLES: Partial<Record<ThemeKey, string>> = {
  kids: 'Kids',
};

// Slugs des catégories TF1+
const TF1_CATEGORY_SLUGS: Partial<Record<ThemeKey, string>> = {
  series:       'series',
  films:        'films',
  documentaire: 'reportages',
  culture:      'divertissement',
  info:         'info',
  sport:        'sport',
  kids:         'jeunesse',
  telerealite:  'divertissement',
  thriller:     'series',
  episodes:     'series',
};

const TF1_CATEGORY_QUERY_ID = '8cc10401301845f170a0f0a8ae75504f5e586232';

// Thèmes pour lesquels un /list est disponible (bouton "Voir plus" affiché)
const THEMES_WITH_LIST = new Set<ThemeKey>([
  'films', 'series', 'documentaire', 'culture', 'info', 'sport',
  'kids', 'sooner', 'telerealite', 'thriller', 'episodes',
]);

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
  const GENERIC_TYPOLOGIES = new Set(['emission', 'spectacle']);

  const TELEREALITE_TOPICS = new Set([
    'telerealite', 'docu-realite', 'mariage', 'famille', 'lifestyle', 'survie', 'aventure',
  ]);

  if (typology && GENERIC_TYPOLOGIES.has(normalizeLabel(typology)) && topics?.length) {
    for (const topic of topics) {
      const t = normalizeLabel(topic);
      if (TELEREALITE_TOPICS.has(t)) return 'telerealite';
    }
    for (const topic of topics) {
      const t = normalizeLabel(topic);
      if (TF1_TOPICS_MAP[t] && TF1_TOPICS_MAP[t] !== 'films') return TF1_TOPICS_MAP[t];
      if (CATEGORY_MAP[t] && CATEGORY_MAP[t] !== 'culture' && CATEGORY_MAP[t] !== 'films') return CATEGORY_MAP[t];
    }
    for (const topic of topics) {
      const t = normalizeLabel(topic);
      if (TF1_TOPICS_MAP[t]) return TF1_TOPICS_MAP[t];
    }
    return 'culture';
  }

  if (typology) {
    const typKey = normalizeLabel(typology);
    const typMapped = CATEGORY_MAP[typKey];
    if (typMapped) {
      const isDramaOrComedy = ['drame', 'comedie', 'comedie dramatique'].includes(typKey);
      if (isDramaOrComedy && durationSec !== undefined) {
        return durationSec > 4800 ? 'films' : 'series';
      }
      return typMapped;
    }
  }

  if (categoryLabel) {
    const key = normalizeLabel(categoryLabel);
    if (key && !GENERIC_TYPOLOGIES.has(key)) {
      const mapped = CATEGORY_MAP[key];
      if (mapped) {
        const isDramaOrComedy = ['drame', 'comedie', 'comedie dramatique'].includes(key);
        if (isDramaOrComedy && durationSec !== undefined) {
          return durationSec > 4800 ? 'films' : 'series';
        }
        return mapped;
      }
      for (const [frag, theme] of Object.entries(CATEGORY_MAP)) {
        if (key.startsWith(frag)) return theme;
      }
      for (const [frag, theme] of Object.entries(CATEGORY_MAP)) {
        if (frag.length >= 6 && key.includes(frag)) return theme;
      }
      if (llmCache[key]) return llmCache[key];
    }
  }

  if (topics?.length) {
    for (const topic of topics) {
      const t = normalizeLabel(topic);
      if (TF1_TOPICS_MAP[t]) return TF1_TOPICS_MAP[t];
      if (CATEGORY_MAP[t]) return CATEGORY_MAP[t];
    }
  }

  return 'series';
}

// ─── Workers AI ───────────────────────────────────────────────────────────────

const LOCAL_FALLBACK_MAP: Record<string, ThemeKey> = {
  'conte': 'kids', 'fable': 'kids', 'marionnettes': 'kids',
  'short': 'films', 'court metrage': 'films',
  'catchup': 'culture', 'replay': 'culture',
  'emission sportive': 'sport', 'magazine sportif': 'sport',
  'actu': 'info', 'flash info': 'info', 'meteo': 'info',
  'sante': 'documentaire', 'medical': 'documentaire',
  'cuisine': 'documentaire', 'gastronomie': 'documentaire',
  'mode': 'culture', 'deco': 'culture',
  'talkshow': 'culture', 'late show': 'culture',
  'docu-serie': 'documentaire', 'serie documentaire': 'documentaire',
  'serie animee': 'kids', 'animation jeunesse': 'kids',
  'comedie romantique': 'films', 'thriller psychologique': 'thriller',
  'serie policiere': 'thriller', 'policiere': 'thriller',
  'aventure sportive': 'sport', 'sport mecanique': 'sport',
  'magazine people': 'culture', 'emission musicale': 'culture',
};

async function classifyWithWorkersAI(
  unknownLabels: string[],
  env: Env,
): Promise<Record<string, ThemeKey>> {
  if (unknownLabels.length === 0) return {};

  const resolved: Record<string, ThemeKey> = {};
  const stillUnknown: string[] = [];

  for (const label of unknownLabels) {
    const k = normalizeLabel(label);
    if (LOCAL_FALLBACK_MAP[k]) {
      resolved[k] = LOCAL_FALLBACK_MAP[k];
    } else {
      let found: ThemeKey | null = null;
      for (const [frag, theme] of Object.entries(LOCAL_FALLBACK_MAP)) {
        if (k.includes(frag) || frag.includes(k)) { found = theme; break; }
      }
      if (found) resolved[k] = found;
      else stillUnknown.push(label);
    }
  }

  if (stillUnknown.length === 0) return resolved;

  const themeKeys = Object.keys(THEMES).join(', ');
  const prompt = `Tu es un classificateur de genres vidéo.
Pour chaque label ci-dessous, retourne le thème le plus proche parmi : ${themeKeys}.
Réponds UNIQUEMENT avec un objet JSON valide, sans texte autour.
Format : { "label": "theme", ... }

Labels :
${stillUnknown.map(l => `- "${l}"`).join('\n')}`;

  try {
    const response = await env.AI.run('@cf/meta/llama-3-8b-instruct', {
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 512,
    });

    const text = (response as any).response ?? '';
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Réponse AI non parseable');

    const result: Record<string, string> = JSON.parse(jsonMatch[0]);
    for (const [label, theme] of Object.entries(result)) {
      if (theme in THEMES) resolved[normalizeLabel(label)] = theme as ThemeKey;
    }
  } catch {
    for (const label of stillUnknown) resolved[normalizeLabel(label)] = 'series';
  }

  return resolved;
}

// ─── Fetch RTBF ───────────────────────────────────────────────────────────────

async function fetchRTBF(): Promise<any> {
  const url = 'https://bff-service.rtbf.be/auvio/v1.23/pages/home?userAgent=Chrome-web-3.0';
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`RTBF ${res.status}`);
  return res.json();
}

async function fetchRTBFWidget(contentPath: string): Promise<any[]> {
  try {
    const url = contentPath.startsWith('http')
      ? contentPath
      : `https://bff-service.rtbf.be${contentPath}`;
    const res = await fetch(
      `${url}${url.includes('?') ? '&' : '?'}_limit=48&_embed=content`,
      { headers: { Accept: 'application/json' } },
    );
    if (!res.ok) return [];
    const json = await res.json();
    return json?.data?.content ?? json?.data ?? [];
  } catch {
    return [];
  }
}

// Fetch une page catégorie RTBF et retourne tous ses items (tous les widgets combinés)
async function fetchRTBFCategoryPage(categoryPath: string): Promise<any[]> {
  const url = `https://bff-service.rtbf.be/auvio/v1.23/pages/categorie/${categoryPath}?userAgent=Chrome-web-3.0`;
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!res.ok) return [];
  const json: any = await res.json();

  const EXCLUDED = new Set([
    'FAVORITE_PROGRAM_LIST', 'CHANNEL_LIST', 'ONGOING_PLAY_HISTORY',
    'CATEGORY_LIST', 'BANNER', 'MEDIA_TRAILER', 'PROMOBOX',
  ]);
  const widgets = (json?.data?.widgets ?? []).filter(
    (w: any) => !EXCLUDED.has(w.type) && w.contentPath,
  );

  const results = await Promise.allSettled(
    widgets.map((w: any) => fetchRTBFWidget(w.contentPath)),
  );
  const items: any[] = [];
  for (const r of results) {
    if (r.status === 'fulfilled') items.push(...r.value);
  }
  return items;
}

// ─── Fetch TF1 ────────────────────────────────────────────────────────────────

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

  const HOME_ID   = 'c34093152db844db6b7ad9b56df12841f7d13182';
  const BANNER_ID = 'bd8e6aab9996844dad4ea9a53887adad27d86151';

  const [homeRes, bannersRes] = await Promise.allSettled([
    fetch(`https://www.tf1.fr/graphql/fr-be/web?id=${HOME_ID}&variables=${homeParams}`, { method: 'GET', headers }),
    fetch(`https://www.tf1.fr/graphql/web?id=${BANNER_ID}`, { method: 'GET', headers }),
  ]);

  const homeJson    = homeRes.status === 'fulfilled' && homeRes.value.ok
    ? await homeRes.value.json() : null;
  const bannersJson = bannersRes.status === 'fulfilled' && bannersRes.value.ok
    ? await bannersRes.value.json() : null;

  return {
    ...(homeJson ?? {}),
    _tf1Banners: bannersJson?.data?.homeCoversByRight ?? [],
  };
}

// Fetch une catégorie TF1 par slug (query categoryBySlug)
async function fetchTF1Category(slug: string): Promise<any[]> {
  const variables = encodeURIComponent(JSON.stringify({
    categorySlug: slug,
    limit: 20,
    ofContentTypes: [
      'ARTICLE', 'CATEGORY', 'CHANNEL', 'COLLECTION', 'EXTERNAL_LINK', 'LANDING_PAGE',
      'LIVE', 'NEXT_BROADCAST', 'PERSONALITY', 'PLAYLIST', 'PLUGIN', 'PROGRAM',
      'PROGRAM_BY_CATEGORY', 'SMART_SUMMARY', 'TOP_PROGRAM', 'TOP_VIDEO', 'TRAILER', 'VIDEO',
    ],
    ofBannerTypes: ['LARGE', 'MEDIUM'],
    ofChannelTypes: ['CORNER', 'DIGITAL', 'EVENT', 'PARTNER', 'TV'],
  }));

  const headers = {
    'content-type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Origin': 'https://www.tf1.fr',
    'Referer': 'https://www.tf1.fr/',
  };

  try {
    const url = `https://www.tf1.fr/graphql/fr-be/web?id=${TF1_CATEGORY_QUERY_ID}&variables=${variables}`;
    const res = await fetch(url, { method: 'GET', headers });
    if (!res.ok) return [];
    const json: any = await res.json();
    return json?.data?.categoryBySlug?.covers ?? [];
  } catch {
    return [];
  }
}

// ─── Normalisation RTBF ───────────────────────────────────────────────────────

function normalizeRTBFItem(item: any, llmCache: Record<string, ThemeKey>, widgetTitle = ''): NormalizedItem | null {
  if (!item || item.resourceType === 'LIVE') return null;

  const widgetTitleLow = widgetTitle.toLowerCase();
  const isKidsWidget   = widgetTitleLow.includes('kids') || widgetTitleLow.includes('enfant') || widgetTitleLow.includes('jeunesse');
  const isSoonerWidget = item.resourceType === 'MEDIA_PREMIUM'
    || (Array.isArray(item.products) && item.products.some((p: any) => p.label === 'Sooner'));

  const baseTheme = isSoonerWidget ? 'sooner'
    : isKidsWidget ? 'kids'
    : resolveTheme(item.categoryLabel, undefined, undefined, item.duration, llmCache);

  const isEpisode = item.type === 'VIDEO' && item.resourceType === 'MEDIA';
  const episodeBuckets = new Set(['series', 'films', 'thriller', 'telerealite']);
  const theme = (isEpisode && episodeBuckets.has(baseTheme)) ? 'episodes' : baseTheme;

  return {
    id: `rtbf-${item.id ?? item.assetId}`,
    title: item.title ?? '',
    subtitle: item.subtitle,
    description: item.description,
    illustration: item.illustration,
    duration: item.duration,
    categoryLabel: item.categoryLabel,
    platform: 'RTBF',
    channelLabel: item.channelLabel,
    resourceType: item.resourceType,
    path: item.path,
    rating: item.rating,
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

function normalizeTF1Item(item: any, llmCache: Record<string, ThemeKey>): NormalizedItem | null {
  if (!item) return null;

  const prog = item.program ?? item;

  const id    = prog.id ?? item.id;
  const title = prog.decoration?.label ?? prog.name ?? item.decoration?.label ?? item.name ?? item.label ?? '';
  if (!title || !id) return null;

  const typology    = prog.typology ?? item.typology ?? '';
  const topics: string[] = prog.topics ?? item.program?.topics ?? [];
  const duration    = item.duration ?? prog.duration ?? 0;
  const rawCategory = typology || (item.__typename === 'Video' ? 'Divertissement' : '');

  const isTopProgram = item.__typename === 'TopProgramItem';
  const theme: ThemeKey = isTopProgram
    ? 'top'
    : resolveTheme(rawCategory || undefined, topics, typology || undefined, duration, llmCache);

  const portraitUrl =
    pickBestUrl(prog.decoration?.portrait?.sourcesWithScales) ??
    pickBestUrl(item.decoration?.portrait?.sourcesWithScales) ??
    pickBestUrl(item.thumbnail?.sourcesWithScales);

  const landscapeUrl =
    pickBestUrl(prog.decoration?.thumbnail?.sourcesWithScales) ??
    pickBestUrl(item.decoration?.thumbnail?.sourcesWithScales) ??
    pickBestUrl(item.image?.sourcesWithScales);

  const useLandscape = LANDSCAPE_BUCKETS.has(theme);
  const primaryUrl = useLandscape
    ? (landscapeUrl ?? portraitUrl)
    : (portraitUrl  ?? landscapeUrl);

  const illustration: Record<string, string> = {};
  if (primaryUrl) {
    illustration.xs = primaryUrl;
    illustration.s  = primaryUrl;
    illustration.m  = primaryUrl;
    illustration.l  = primaryUrl;
    illustration.xl = primaryUrl;
  }

  const isVideo      = item.__typename === 'Video';
  const isFilm       = typology === 'Film';
  const resourceType = (isVideo || isFilm) ? 'MEDIA' : 'PROGRAM';

  const ctaVideoId = item.callToAction?.items?.find(
    (i: any) => i.type === 'PLAY' || i.__typename === 'WatchButtonAction'
  )?.video?.id ?? null;

  const mediaId  = isVideo ? id : (isFilm ? (ctaVideoId ?? id) : undefined);
  const streamId = mediaId;
  const assetId  = mediaId;

  const hasSubtitles        = (prog.hasFrenchDeafSubtitles?.total ?? 0) > 0
                            || (prog.hasFrenchSubtitles?.total ?? 0) > 0;
  const hasAudioDescriptions = (prog.hasDescriptionTrack?.total ?? 0) > 0;

  let rating: string | null = prog.rating ?? item.rating ?? null;
  if (rating) rating = rating.replace('CSA_', '').replace('ALL', 'Tout public');

  const allBadges = [...(item.badges ?? []), ...(item.editorBadges ?? [])];
  const stamp = allBadges.length > 0 ? {
    label:           allBadges[0].label ?? allBadges[0].type ?? '',
    backgroundColor: '#1a56db',
    textColor:       '#fff',
  } : undefined;

  const enrichedRaw = {
    ...item,
    id,
    title,
    subtitle:    prog.decoration?.catchPhrase ?? item.decoration?.catchPhrase,
    description: prog.synopsis ?? prog.decoration?.description ?? item.synopsis,
    illustration: Object.keys(illustration).length > 0 ? illustration : undefined,
    duration,
    typology,
    slug:        prog.slug ?? item.slug,
    programId:   prog.id,
    programSlug: prog.slug,
    resourceType,
    platform: 'TF1+',
    streamId,
    assetId,
    hasSubtitles,
    hasAudioDescriptions,
    rating,
    stamp,
    ...(isFilm ? { isFilm: true } : {}),
  };

  return {
    id: `tf1-${id}`,
    title,
    subtitle:     enrichedRaw.subtitle,
    description:  enrichedRaw.description,
    illustration: enrichedRaw.illustration,
    duration,
    categoryLabel: rawCategory || typology || 'TF1+',
    platform: 'TF1+',
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
  const rtbfGroups = new Map<ThemeKey, NormalizedItem[]>();
  const tf1Groups  = new Map<ThemeKey, NormalizedItem[]>();
  for (const theme of BUCKET_ORDER) {
    rtbfGroups.set(theme, []);
    tf1Groups.set(theme, []);
  }

  for (const item of rtbfItems) {
    if (rtbfGroups.has(item.theme)) rtbfGroups.get(item.theme)!.push(item);
    else rtbfGroups.set(item.theme, [item]);
  }
  for (const item of tf1Items) {
    if (tf1Groups.has(item.theme))  tf1Groups.get(item.theme)!.push(item);
    else tf1Groups.set(item.theme, [item]);
  }

  return BUCKET_ORDER
    .map(theme => {
      const rtbf = rtbfGroups.get(theme) ?? [];
      const tf1  = tf1Groups.get(theme)  ?? [];

      const merged: NormalizedItem[] = [];
      const maxR = rtbf.length, maxT = tf1.length;
      let r = 0, t = 0;
      while (r < maxR || t < maxT) {
        if (r < maxR) merged.push(rtbf[r++]);
        if (t < maxT) merged.push(tf1[t++]);
      }

      return {
        theme,
        label: THEMES[theme].label,
        emoji: THEMES[theme].emoji,
        items: merged,
        hasMore: THEMES_WITH_LIST.has(theme),
      };
    })
    .filter(b => b.items.length > 0);
}

// ─── Banners helpers ──────────────────────────────────────────────────────────

function buildRTBFBanners(rtbfHome: any, promoboxItems: any[] = []): any[] {
  const banners: any[] = [];

  const items = promoboxItems.length > 0 ? promoboxItems : (() => {
    const allWidgets = rtbfHome?.data?.widgets ?? [];
    for (const w of allWidgets) {
      if (w.type === 'PROMOBOX' && Array.isArray(w.data?.content)) return w.data.content as any[];
    }
    return [] as any[];
  })();

  for (const d of items) {
    const dl: string = d.deeplink ?? '';

    let contentType: string = d.resourceType ?? 'media';
    let contentId:   string = String(d.resourceValue ?? d.mediaId ?? '');
    let contentSlug: string | null = null;

    const medMatch  = dl.match(/^\/media\/(.+)-(\d+)$/);
    const emMatch   = dl.match(/^\/emission\/(.+)-(\d+)$/);
    const progMatch = dl.match(/^\/program(?:me)?\/(.+)-(\d+)$/);
    if (medMatch)        { contentType = 'media';   contentId = medMatch[2];  contentSlug = medMatch[1]; }
    else if (emMatch)    { contentType = 'program'; contentId = emMatch[2];   contentSlug = emMatch[1]; }
    else if (progMatch)  { contentType = 'program'; contentId = progMatch[2]; contentSlug = progMatch[1]; }

    if (!d.title) continue;

    const image = d.image
      ? {
          xs: d.image.xs ?? d.image.s ?? '',
          s:  d.image.s  ?? '',
          m:  d.image.m  ?? '',
          l:  d.image.l  ?? '',
          xl: d.image.xl ?? d.image.l ?? '',
        }
      : null;

    banners.push({
      id:          `rtbf-banner-${contentId || Math.random()}`,
      coverId:     contentId,
      title:       d.title,
      subtitle:    d.subtitle ?? '',
      description: d.description ?? '',
      image,
      videoUrl:    null,
      deepLink:    dl || null,
      contentType,
      contentId,
      contentSlug,
      backgroundColor: d.backgroundColor ?? '#000000',
      platform: 'RTBF',
    });
  }
  return banners;
}

function buildTF1Banners(tf1Raw: any): any[] {
  const banners: any[] = [];
  const covers: any[] = tf1Raw?._tf1Banners ?? [];

  for (const cover of covers.slice(0, 6)) {
    const deco = cover.decoration ?? {};
    const prog = cover.program ?? {};
    const vid  = cover.video  ?? {};

    const id    = cover.id ?? prog.id ?? vid.id;
    const title = deco.label ?? prog.name ?? vid.program?.name ?? '';
    if (!id || !title) continue;

    const desc = deco.description ?? deco.catchPhrase ?? deco.summary ?? '';

    const bgSrcs   = [...(deco.cover?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 1) - (a.scale ?? 1));
    const portSrcs = [...(deco.coverSmall?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 1) - (a.scale ?? 1));

    const bgBest   = bgSrcs.find((s: any) => s.scale >= 2)?.url ?? bgSrcs[0]?.url ?? '';
    const bgBase   = bgSrcs[0]?.url ?? '';
    const portBest = portSrcs.find((s: any) => s.scale >= 2)?.url ?? portSrcs[0]?.url ?? '';
    const portBase = portSrcs[0]?.url ?? '';

    const image = (bgBest || portBest) ? {
      xs: portBase || bgBase,
      s:  portBest || bgBase,
      m:  bgBase   || portBase,
      l:  bgBest   || portBest,
      xl: bgBest   || portBest,
    } : null;

    const videoUrl = deco.video?.sources?.[0]?.url ?? null;

    const typename = cover.__typename ?? '';
    let contentType: string;
    let contentId:   string;
    let contentSlug: string | null;
    let programId:   string | null = prog.id ?? null;
    let programSlug: string | null = prog.slug ?? null;

    if (typename === 'CoverOfVideo' || cover.contentType === 'VIDEO') {
      contentType  = 'video';
      contentId    = vid.id ?? id;
      contentSlug  = vid.slug ?? cover.contentSlug ?? null;
    } else {
      contentType = 'program';
      const ctaPlay = cover.callToAction?.items?.find((i: any) => i.type === 'PLAY' || i.__typename === 'WatchButtonAction');
      contentId    = ctaPlay?.video?.id ?? ctaPlay?.id ?? prog.id ?? id;
      contentSlug  = ctaPlay?.video?.slug ?? prog.slug ?? cover.contentSlug ?? null;
    }

    const ctaVideo = cover.callToAction?.items?.find((i: any) => i.video)?.video ?? {};
    const season   = vid.season   ?? ctaVideo?.season   ?? null;
    const episode  = vid.episode  ?? ctaVideo?.episode  ?? null;
    const duration = vid.playingInfos?.duration ?? ctaVideo?.playingInfos?.duration ?? null;
    const rights   = vid.rights   ?? ctaVideo?.rights   ?? cover.rights ?? [];

    const topicsSource = (typename === 'CoverOfVideo' ? vid.program : prog) ?? prog;

    banners.push({
      id: `tf1-banner-${id}`,
      coverId: String(id),
      title,
      description: desc,
      image,
      videoUrl,
      deepLink: null,
      contentType,
      contentId:   String(contentId ?? id),
      contentSlug: contentSlug ?? null,
      programId,
      programSlug,
      typology:  topicsSource.typology  ?? prog.typology  ?? '',
      topics:    topicsSource.topics    ?? prog.topics    ?? [],
      season,
      episode,
      duration,
      rights,
      backgroundColor: '#000000',
      platform: 'TF1+',
    });
  }

  if (banners.length === 0) {
    const sliders: any[] = tf1Raw?.data?.homeSliders ?? [];
    for (const item of (sliders[0]?.items ?? []).slice(0, 5)) {
      const prog  = item.program ?? item;
      const id    = prog.id ?? item.id;
      const title = prog.decoration?.label ?? prog.name ?? '';
      if (!id || !title) continue;

      const desc     = prog.decoration?.catchPhrase ?? '';
      const thumbSrc = [...(prog.decoration?.thumbnail?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 0) - (a.scale ?? 0));
      const portSrc  = [...(prog.decoration?.portrait?.sourcesWithScales  ?? [])].sort((a: any, b: any) => (b.scale ?? 0) - (a.scale ?? 0));

      const bgUrl    = thumbSrc[0]?.url ?? '';
      const portUrl  = portSrc[0]?.url ?? '';

      banners.push({
        id: `tf1-banner-${id}`, coverId: String(id),
        title, description: desc,
        image: (bgUrl || portUrl) ? { xs: portUrl || bgUrl, s: portUrl || bgUrl, m: bgUrl || portUrl, l: bgUrl || portUrl, xl: bgUrl || portUrl } : null,
        videoUrl: null, deepLink: null,
        contentType: 'program', contentId: id,
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
  corsHeaders: Record<string, string>,
): Promise<Response> {
  const theme = url.searchParams.get('theme') as ThemeKey | null;

  if (!theme || !(theme in THEMES)) {
    return new Response(JSON.stringify({ error: 'Paramètre theme invalide ou manquant' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  // Récupérer le cache LLM
  const llmCacheRaw = await env.LABEL_CACHE.get('label_map').catch(() => null);
  const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

  const rtbfCategoryPath = RTBF_CATEGORY_PATHS[theme];
  const rtbfWidgetId     = RTBF_WIDGET_IDS[theme];
  const tf1Slug          = TF1_CATEGORY_SLUGS[theme];
  const widgetTitle      = RTBF_WIDGET_FORCE_TITLES[theme] ?? '';

  // Fetch RTBF + TF1 en parallèle
  const [rtbfResult, tf1Result] = await Promise.allSettled([
    rtbfCategoryPath
      ? fetchRTBFCategoryPage(rtbfCategoryPath)
      : rtbfWidgetId
        ? fetchRTBFWidget(`https://bff-service.rtbf.be/auvio/v1.23/widgets/${rtbfWidgetId}`)
        : Promise.resolve([]),
    tf1Slug ? fetchTF1Category(tf1Slug) : Promise.resolve([]),
  ]);

  // Normaliser RTBF
  let rtbfItems: NormalizedItem[] = [];
  if (rtbfResult.status === 'fulfilled') {
    for (const raw of rtbfResult.value) {
      const item = normalizeRTBFItem(raw, llmCache, widgetTitle);
      if (item) rtbfItems.push(item);
    }
    rtbfItems = deduplicate(rtbfItems);
  }

  // Normaliser TF1
  let tf1Items: NormalizedItem[] = [];
  if (tf1Result.status === 'fulfilled') {
    for (const raw of tf1Result.value) {
      const item = normalizeTF1Item(raw, llmCache);
      if (item) tf1Items.push(item);
    }
    tf1Items = deduplicate(tf1Items);
  }

  // Interleave RTBF + TF1
  const merged: NormalizedItem[] = [];
  let r = 0, t = 0;
  while (r < rtbfItems.length || t < tf1Items.length) {
    if (r < rtbfItems.length) merged.push(rtbfItems[r++]);
    if (t < tf1Items.length)  merged.push(tf1Items[t++]);
  }

  console.log(`[/list] theme=${theme} rtbf=${rtbfItems.length} tf1=${tf1Items.length} total=${merged.length}`);

  return new Response(JSON.stringify({
    theme,
    label: THEMES[theme].label,
    emoji: THEMES[theme].emoji,
    items: merged,
    meta: {
      rtbf:  rtbfItems.length,
      tf1:   tf1Items.length,
      total: merged.length,
    },
  }), {
    headers: {
      ...corsHeaders,
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=300',
    },
  });
}

// ─── Handler principal ────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // ── Route /list ──────────────────────────────────────────────────────────
    if (url.pathname === '/list') {
      try {
        return await handleListRequest(url, env, corsHeaders);
      } catch (err: any) {
        console.error('[worker] /list error:', err);
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }
    }

    // ── Route /home ──────────────────────────────────────────────────────────
    if (url.pathname !== '/home') {
      return new Response('Not found', { status: 404, headers: corsHeaders });
    }

    try {
      // 1. Fetch RTBF + TF1 en parallèle
      const [rtbfResult, tf1Result] = await Promise.allSettled([
        fetchRTBF(),
        fetchTF1(),
      ]);

      const rtbfHome = rtbfResult.status === 'fulfilled' ? rtbfResult.value : null;
      const tf1Raw   = tf1Result.status  === 'fulfilled' ? tf1Result.value  : null;

      if (rtbfResult.status === 'rejected') console.error('[worker] RTBF fetch error:', rtbfResult.reason);
      if (tf1Result.status  === 'rejected') console.error('[worker] TF1 fetch error:',  tf1Result.reason);

      // 2. Banners hero
      let rtbfPromoItems: any[] = [];
      if (rtbfHome) {
        const promoWidget = (rtbfHome.data?.widgets ?? []).find((w: any) => w.type === 'PROMOBOX');
        if (promoWidget?.contentPath) {
          try {
            const pUrl = promoWidget.contentPath.startsWith('http')
              ? promoWidget.contentPath
              : `https://bff-service.rtbf.be${promoWidget.contentPath}`;
            const pRes = await fetch(pUrl, { headers: { Accept: 'application/json' } });
            if (pRes.ok) {
              const pJson: any = await pRes.json();
              rtbfPromoItems = pJson?.data?.content ?? pJson?.data ?? [];
            }
          } catch (e) {
            console.error('[worker] RTBF PROMOBOX fetch error:', e);
          }
        }
      }

      const heroBanners: any[] = [
        ...(rtbfHome ? buildRTBFBanners(rtbfHome, rtbfPromoItems) : []),
        ...(tf1Raw   ? buildTF1Banners(tf1Raw)                    : []),
      ];

      // 3. Cache LLM
      const llmCacheRaw = await env.LABEL_CACHE.get('label_map').catch(() => null);
      const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

      // 4. Normaliser RTBF
      let rtbfItems: NormalizedItem[] = [];
      if (rtbfHome) {
        const EXCLUDED = new Set([
          'FAVORITE_PROGRAM_LIST', 'CHANNEL_LIST', 'ONGOING_PLAY_HISTORY',
          'CATEGORY_LIST', 'BANNER', 'MEDIA_TRAILER', 'PROMOBOX',
        ]);
        const widgets = rtbfHome.data?.widgets ?? [];

        const widgetMetas = widgets
          .filter((w: any) => !EXCLUDED.has(w.type) && w.contentPath)
          .map((w: any) => ({ title: w.title ?? '', fetch: fetchRTBFWidget(w.contentPath) }));

        const fetches      = widgetMetas.map((m: any) => m.fetch);
        const widgetTitles = widgetMetas.map((m: any) => m.title);

        const results = await Promise.allSettled(fetches);
        for (let i = 0; i < results.length; i++) {
          const r = results[i];
          if (r.status !== 'fulfilled') continue;
          const widgetTitle = widgetTitles[i] ?? '';
          for (const raw of r.value) {
            const item = normalizeRTBFItem(raw, llmCache, widgetTitle);
            if (item) rtbfItems.push(item);
          }
        }
      }

      // 5. Normaliser TF1
      let tf1Items: NormalizedItem[] = [];
      if (tf1Raw) {
        const sliders = tf1Raw.data?.homeSliders ?? [];
        for (const slider of sliders) {
          for (const item of slider.items ?? []) {
            const normalized = normalizeTF1Item(item, llmCache);
            if (normalized) tf1Items.push(normalized);
          }
        }
      }

      // 6. Déduplication
      rtbfItems = deduplicate(rtbfItems);
      tf1Items  = deduplicate(tf1Items);

      // 7. AI pour labels inconnus résiduels
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
          const updatedCache = { ...llmCache, ...newMappings };
          await env.LABEL_CACHE.put('label_map', JSON.stringify(updatedCache), { expirationTtl: 604800 });

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

      // 8. Buckets
      const buckets = buildBuckets(rtbfItems, tf1Items);

      // 9. Répondre
      console.log(`[worker] rtbf=${rtbfItems.length} tf1=${tf1Items.length} buckets=${buckets.length} ai_calls=${unknownLabels.length}`);

      return new Response(JSON.stringify({
        buckets,
        heroBanners,
        meta: {
          rtbf: rtbfItems.length,
          tf1:  tf1Items.length,
          buckets: buckets.length,
          unknownLabelsClassifiedByAI: Object.keys(newMappings),
          totalUnknownLabels: unknownLabels.length,
        },
      }), {
        headers: {
          ...corsHeaders,
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=300',
        },
      });

    } catch (err: any) {
      console.error('[worker] Fatal error:', err);
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }
  },
};
