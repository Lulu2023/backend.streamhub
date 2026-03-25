// index.ts - Backend StreamHub (Cloudflare Workers)
// Platforms: RTBF Auvio, TF1+, RTL Play, France.tv

// ─────────────────────────────────────────────────────────────────────────────
// TYPES & CONSTANTS
// ─────────────────────────────────────────────────────────────────────────────

export type ThemeKey = 
  | 'films' | 'series' | 'documentaire' | 'info' | 'sport' 
  | 'kids' | 'divertissement' | 'culture' | 'musique' | 'top';

export interface NormalizedItem {
  id: string;
  title: string;
  subtitle: string | null;
  description: string;
  illustration: string | null;
  duration: number | null;
  categoryLabel: string;
  platform: 'RTBF' | 'TF1+' | 'RTL Play' | 'France.tv';
  channelLabel: string;
  resourceType: 'PROGRAM' | 'MEDIA' | 'LIVE' | 'COLLECTION';
  path: string;
  theme: ThemeKey;
  genres: string[];
  year?: number;
  badges?: string[];
  _raw?: any;
}

export interface HomeResponse {
  buckets: {
    title: string;
    theme?: ThemeKey;
    items: NormalizedItem[];
    platform?: string;
    widgetId?: string;
  }[];
  heroBanners: NormalizedItem[];
  meta: {
    rtbf: number;
    tf1: number;
    rtlplay: number;
    francetv: number;
    buckets: number;
    builtAt: number;
  };
}

export interface ListResponse {
  items: NormalizedItem[];
  meta: {
    theme: ThemeKey;
    page: number;
    hasMore: boolean;
    platforms: string[];
    total?: number;
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// CONFIGURATION PAR PLATEFORME
// ─────────────────────────────────────────────────────────────────────────────

// RTBF Auvio
const RTBF_BASE = 'https://bff-service.rtbf.be/auvio/v1.23';
const RTBF_HEADERS = {
  'Accept': 'application/json',
  'User-Agent': 'Chrome-web-3.0',
};

const RTBF_WIDGETS: Record<string, { id: string; title: string; theme?: ThemeKey }> = {
  '19764': { id: '19764', title: 'Films & Téléfilms', theme: 'films' },
  '22683': { id: '22683', title: 'Séries', theme: 'series' },
  '20691': { id: '20691', title: 'Documentaires', theme: 'documentaire' },
  '20114': { id: '20114', title: 'Info', theme: 'info' },
  '22709': { id: '22709', title: 'Sport', theme: 'sport' },
  '22390': { id: '22390', title: 'Kids', theme: 'kids' },
  '20136': { id: '20136', title: 'Culture & Lifestyle', theme: 'culture' },
  '24272': { id: '24272', title: 'Podcasts', theme: 'divertissement' },
};

// TF1+
const TF1_GRAPHQL_BASE = 'https://www.tf1.fr/graphql/fr-be/web';
const TF1_HEADERS = {
  'Accept': 'application/json',
  'Content-Type': 'application/json',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
};

const TF1_CATEGORY_SLUGS: Record<ThemeKey, string | null> = {
  films: 'films',
  series: 'series',
  documentaire: 'documentaires',
  info: 'info',
  sport: 'sport',
  kids: 'kids',
  divertissement: 'divertissement',
  culture: 'culture',
  musique: 'musique',
  top: null,
};

// RTL Play
const RTLPLAY_BASE = 'https://lfvp-api.dpgmedia.net/RTL_PLAY/storefronts';
const RTLPLAY_HEADERS = {
  'User-Agent': 'RTL_PLAY/23.251217 (com.tapptic.rtl.tvi; build:26234; Android 30)',
  'Accept': 'application/json',
  'lfvp-device-segment': 'TV>Android',
  'x-app-version': '23',
};

const RTLPLAY_SLUGS: Record<ThemeKey, string | null> = {
  films: 'films',
  series: 'series',
  documentaire: null,
  info: 'info',
  sport: 'sport',
  kids: 'kids',
  divertissement: 'divertissement',
  culture: null,
  musique: null,
  top: null,
};

// France.tv
const FRANCETV_BASE = 'https://api-mobile.yatta.francetv.fr';
const FRANCETV_HEADERS = {
  'Accept': 'application/json',
  'User-Agent': 'FranceTV/6.0 (iOS; mobile)',
};

const FRANCETV_SLUGS: Record<ThemeKey, string | null> = {
  films: 'films',
  series: 'series-et-fictions',
  documentaire: 'documentaires',
  info: 'info',
  sport: 'sport',
  kids: 'enfants',
  divertissement: 'jeux-et-divertissements',
  culture: 'spectacles-et-culture',
  musique: null,
  top: null,
};

// ─────────────────────────────────────────────────────────────────────────────
// UTILITAIRES DE FETCH AVEC RETRY & FALLBACK
// ─────────────────────────────────────────────────────────────────────────────

async function fetchWithRetry(
  url: string,
  options: RequestInit = {},
  maxRetries = 3,
  platform: string,
  cacheKey?: string,
  env?: Env
): Promise<Response> {
  let lastError: Error | null = null;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      // Vérifier cache avant fetch si disponible
      if (cacheKey && env?.DATA_CACHE) {
        const cached = await env.DATA_CACHE.get(cacheKey);
        if (cached) {
          console.log(`[${platform}] Cache hit: ${cacheKey}`);
          return new Response(cached, {
            headers: { 'Content-Type': 'application/json', 'X-Cache': 'HIT' },
          });
        }
      }
      
      const res = await fetch(url, {
        ...options,
        headers: {
          'Accept': 'application/json',
          ...options.headers,
        },
        cf: { cacheTtl: 300 }, // Cloudflare edge cache 5min
      });
      
      if (res.ok) {
        // Mettre en cache si demandé
        if (cacheKey && env?.DATA_CACHE) {
          const body = await res.clone().text();
          await env.DATA_CACHE.put(cacheKey, body, { expirationTtl: 60 * 30 });
        }
        return res;
      }
      
      console.warn(`[${platform}] Attempt ${attempt}: HTTP ${res.status} for ${url}`);
      lastError = new Error(`HTTP ${res.status}`);
    } catch (err) {
      console.warn(`[${platform}] Attempt ${attempt} failed:`, (err as Error).message);
      lastError = err as Error;
    }
    
    // Exponential backoff
    if (attempt < maxRetries) {
      await new Promise(resolve => setTimeout(resolve, 500 * Math.pow(2, attempt - 1)));
    }
  }
  
  throw lastError ?? new Error(`Failed after ${maxRetries} attempts`);
}

async function safeFetch<T>(
  fetchFn: () => Promise<T>,
  platform: string,
  fallback: T,
  env?: Env,
  cacheKey?: string
): Promise<T> {
  try {
    return await fetchFn();
  } catch (err) {
    console.error(`[${platform}] Fetch error:`, (err as Error).message);
    
    // Fallback vers cache si disponible
    if (cacheKey && env?.DATA_CACHE) {
      const cached = await env.DATA_CACHE.get(cacheKey);
      if (cached) {
        console.log(`[${platform}] Using cached fallback`);
        return JSON.parse(cached) as T;
      }
    }
    
    return fallback;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// VOCABULAIRE DE GENRES UNIFIÉ
// ─────────────────────────────────────────────────────────────────────────────

const GENRE_VOCABULARY: Record<string, string> = {
  // Films
  'film': 'Film', 'films': 'Film', 'cinema': 'Film', 'long metrage': 'Film',
  'telefilm': 'Téléfilm', 'tv movie': 'Téléfilm',
  
  // Séries
  'serie': 'Série', 'series': 'Série', 'feuilleton': 'Série', 'soap': 'Série',
  'sitcom': 'Série', 'drama': 'Série',
  
  // Genres cinématographiques
  'action': 'Action', 'aventure': 'Aventure', 'thriller': 'Thriller',
  'policier': 'Policier', 'crime': 'Policier', 'enquete': 'Policier',
  'comedy': 'Comédie', 'comedie': 'Comédie', 'humour': 'Comédie',
  'drame': 'Drame', 'romance': 'Romance', 'amour': 'Romance',
  'sf': 'Science-Fiction', 'science fiction': 'Science-Fiction', 'fantasy': 'Fantastique',
  'fantastique': 'Fantastique', 'horreur': 'Horreur', 'epouvante': 'Horreur',
  'animation': 'Animation', 'anime': 'Animation', 'dessin anime': 'Animation',
  'documentaire': 'Documentaire', 'doc': 'Documentaire', 'reportage': 'Documentaire',
  
  // Divertissement
  'emission': 'Émission', 'variety': 'Variétés', 'varietes': 'Variétés',
  'talk': 'Talk-show', 'magazine': 'Magazine', 'jeux': 'Jeux', 'quiz': 'Jeux',
  'tele-realite': 'Télé-réalité', 'reality': 'Télé-réalité',
  
  // Info & Société
  'info': 'Actualité', 'actualite': 'Actualité', 'journal': 'Actualité',
  'societe': 'Société', 'politique': 'Politique', 'economie': 'Économie',
  
  // Sport
  'sport': 'Sport', 'football': 'Football', 'tennis': 'Tennis', 'rugby': 'Rugby',
  
  // Jeunesse
  'kids': 'Jeunesse', 'enfants': 'Jeunesse', 'junior': 'Jeunesse', 'ado': 'Jeunesse',
  
  // Culture
  'culture': 'Culture', 'art': 'Culture', 'histoire': 'Culture', 'patrimoine': 'Culture',
  'musique': 'Musique', 'concert': 'Concert', 'chanson': 'Musique',
  
  // Santé & Science
  'sante': 'Santé', 'science': 'Science', 'nature': 'Nature', 'environnement': 'Nature',
};

function normalizeLabel(label: string): string {
  return label
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-');
}

function buildGenres(
  categoryLabel?: string,
  typology?: string,
  topics?: string[],
  tags?: string[]
): string[] {
  const raw = [categoryLabel, typology, ...(topics ?? []), ...(tags ?? [])]
    .filter(Boolean)
    .map(s => normalizeLabel(String(s)));
  
  const genres = raw
    .map(lbl => GENRE_VOCABULARY[lbl])
    .filter(Boolean) as string[];
  
  return [...new Set(genres)];
}

function inferTheme(
  categoryLabel?: string,
  typology?: string,
  topics?: string[],
  duration?: number
): ThemeKey {
  const label = normalizeLabel(categoryLabel ?? typology ?? '');
  
  if (label.includes('film') || label.includes('cinema')) return 'films';
  if (label.includes('serie') || label.includes('feuilleton')) return 'series';
  if (label.includes('doc')) return 'documentaire';
  if (label.includes('info') || label.includes('actualite') || label.includes('journal')) return 'info';
  if (label.includes('sport')) return 'sport';
  if (label.includes('kid') || label.includes('enfant') || label.includes('junior')) return 'kids';
  if (label.includes('musique') || label.includes('concert')) return 'musique';
  if (label.includes('culture') || label.includes('art') || label.includes('histoire')) return 'culture';
  if (label.includes('divertissement') || label.includes('emission') || label.includes('variety')) return 'divertissement';
  
  // Fallback par durée
  if (duration && duration > 3600) return 'films';
  if (duration && duration < 1800) return 'divertissement';
  
  return 'divertissement';
}

// ─────────────────────────────────────────────────────────────────────────────
// NORMALISATION RTBF
// ─────────────────────────────────────────────────────────────────────────────

function normalizeRTBFItem(item: any, widgetTitle = ''): NormalizedItem | null {
  if (!item?.id) return null;
  
  const prog = item.program ?? item;
  const title = prog.title ?? prog.label ?? prog.name ?? '';
  if (!title) return null;
  
  // Images
  const illustration = prog.illustration?.['16x9']?.['m'] 
    ?? prog.illustration?.['16x9']?.['s']
    ?? prog.image?.url
    ?? null;
  
  // Métadonnées
  const duration = prog.duration ?? prog.metadata?.duration ?? null;
  const year = prog.productionYear ?? null;
  const categoryLabel = prog.category?.label ?? prog.genre ?? widgetTitle;
  const resourceType = prog.resourceType ?? (prog.type === 'VIDEO' ? 'MEDIA' : 'PROGRAM');
  
  return {
    id: `rtbf-${item.id}`,
    title,
    subtitle: prog.subtitle ?? prog.originalTitle ?? null,
    description: prog.description ?? prog.synopsis ?? prog.catchPhrase ?? '',
    illustration,
    duration,
    categoryLabel,
    platform: 'RTBF',
    channelLabel: prog.channel?.label ?? 'RTBF Auvio',
    resourceType,
    path: `/rtbf/${resourceType === 'MEDIA' ? 'video' : 'program'}/${item.id}`,
    theme: inferTheme(categoryLabel, prog.typology, prog.topics, duration),
    genres: buildGenres(categoryLabel, prog.typology, prog.topics),
    year: year ?? undefined,
    badges: prog.badges?.map((b: any) => b.label) ?? [],
    _raw: item,
  };
}

async function fetchRTBFHome(): Promise<any> {
  const res = await fetchWithRetry(
    `${RTBF_BASE}/pages/home?userAgent=Chrome-web-3.0`,
    { headers: RTBF_HEADERS },
    3,
    'RTBF',
    'rtbf_home',
    env
  );
  return res.json();
}

async function fetchRTBFWidget(widgetId: string): Promise<any[]> {
  const res = await fetchWithRetry(
    `${RTBF_BASE}/widgets/${widgetId}?userAgent=Chrome-web-3.0`,
    { headers: RTBF_HEADERS },
    3,
    'RTBF',
    `rtbf_widget_${widgetId}`,
    env
  );
  const json = await res.json();
  return json?.data?.content?.items ?? json?.data?.items ?? [];
}

// ─────────────────────────────────────────────────────────────────────────────
// NORMALISATION TF1+
// ─────────────────────────────────────────────────────────────────────────────

function extractImageSrc(sourcesWithScales?: any[]): string | null {
  if (!sourcesWithScales?.length) return null;
  // Priorité: avif > jpg, scale 2 > 1
  const preferred = sourcesWithScales.find((s: any) => 
    s.type === 'avif' && s.scale === 2
  ) ?? sourcesWithScales.find((s: any) => 
    s.type === 'jpg' && s.scale === 2
  ) ?? sourcesWithScales[0];
  return preferred?.url ?? null;
}

function normalizeTF1Item(item: any, widgetTitle = ''): NormalizedItem | null {
  if (!item?.id) return null;
  
  const prog = item.program ?? item;
  const decoration = prog.decoration ?? item.decoration ?? {};
  
  const title = prog.title ?? decoration.title ?? item.title ?? '';
  if (!title) return null;
  
  // Images
  const portraitSrc = extractImageSrc(
    item.thumbnail?.sourcesWithScales ??
    prog.decoration?.portrait?.sourcesWithScales ??
    decoration.coverSmall?.sourcesWithScales
  );
  
  // Métadonnées
  const duration = prog.duration ?? item.duration ?? prog.playingInfos?.duration ?? null;
  const year = prog.productionYear ?? null;
  const categoryLabel = prog.typology ?? item.typology ?? widgetTitle;
  const topics = prog.topics ?? item.program?.topics ?? [];
  
  // Badges
  const badges = item.badges?.map((b: any) => b.label) ?? [];
  
  return {
    id: `tf1-${item.id}`,
    title,
    subtitle: decoration.catchPhrase ?? prog.subtitle ?? null,
    description: decoration.description ?? prog.description ?? '',
    illustration: portraitSrc,
    duration,
    categoryLabel,
    platform: 'TF1+',
    channelLabel: prog.publisher?.label ?? 'TF1+',
    resourceType: prog.__typename === 'Video' ? 'MEDIA' : 'PROGRAM',
    path: `/tf1/${prog.__typename === 'Video' ? 'video' : 'program'}/${item.id}`,
    theme: inferTheme(categoryLabel, prog.typology, topics, duration),
    genres: buildGenres(categoryLabel, prog.typology, topics),
    year: year ?? undefined,
    badges,
    _raw: item,
  };
}

async function fetchTF1Home(): Promise<any> {
  const variables = {
    ofBannerTypes: [],
    ofContentTypes: ['TOP_PROGRAM', 'PROGRAM'],
    ofChannelTypes: [],
  };
  
  const url = `${TF1_GRAPHQL_BASE}?id=c34093152db844db6b7ad9b56df12841f7d13182&variables=${encodeURIComponent(JSON.stringify(variables))}`;
  
  const res = await fetchWithRetry(url, { headers: TF1_HEADERS }, 3, 'TF1+', 'tf1_home', env);
  return res.json();
}

async function fetchTF1Category(slug: string): Promise<any[]> {
  const variables = {
    categorySlug: slug,
    limit: 50,
    ofContentTypes: ['ARTICLE', 'CATEGORY', 'CHANNEL', 'COLLECTION', 'EXTERNAL_LINK', 'LANDING_PAGE', 'LIVE', 'NEXT_BROADCAST', 'PERSONALITY', 'PLAYLIST', 'PLUGIN', 'PROGRAM', 'PROGRAM_BY_CATEGORY', 'SMART_SUMMARY', 'TOP_PROGRAM', 'TOP_VIDEO', 'TRAILER', 'VIDEO'],
    ofBannerTypes: ['LARGE', 'MEDIUM'],
    ofChannelTypes: ['CORNER', 'DIGITAL', 'EVENT', 'PARTNER', 'TV'],
  };
  
  const url = `${TF1_GRAPHQL_BASE}?id=46f87e88577a61abb1d2a36a715a12d4175caa3d&variables=${encodeURIComponent(JSON.stringify(variables))}`;
  
  const res = await fetchWithRetry(url, { headers: TF1_HEADERS }, 3, 'TF1+', `tf1_cat_${slug}`, env);
  const json = await res.json();
  
  // Extraire les items des sliders
  const sliders = json?.data?.page?.content?.sliders ?? [];
  return sliders.flatMap((slider: any) => slider.items ?? []).filter((item: any) => item?.id);
}

// ─────────────────────────────────────────────────────────────────────────────
// NORMALISATION RTL PLAY
// ─────────────────────────────────────────────────────────────────────────────

function normalizeRTLPlayItem(item: any, widgetTitle = ''): NormalizedItem | null {
  if (!item?.id && !item?.detailId) return null;
  
  const id = item.id ?? item.detailId;
  const title = item.title ?? '';
  if (!title) return null;
  
  // Images
  const illustration = item.heroImageUrl 
    ?? item.imageUrl 
    ?? item.mobileImageUrl
    ?? item.landscapeImageUrl
    ?? null;
  
  // Durée: extraire depuis labels "XX min"
  let duration: number | null = null;
  if (Array.isArray(item.labels)) {
    const durLabel = item.labels.find((l: any) => l.label?.includes('min'));
    if (durLabel?.label) {
      const match = durLabel.label.match(/(\d+)\s*min/);
      if (match) duration = parseInt(match[1]) * 60;
    }
  }
  
  // Année
  let year: number | undefined;
  if (Array.isArray(item.labels)) {
    const yearLabel = item.labels.find((l: any) => l.accessibilityLabel === 'Année de production');
    if (yearLabel?.label) {
      year = parseInt(yearLabel.label);
    }
  }
  
  const categoryLabel = item.category ?? item.genre ?? widgetTitle;
  
  return {
    id: `rtlplay-${id}`,
    title,
    subtitle: item.originalTitle ?? null,
    description: item.description ?? item.synopsis ?? '',
    illustration,
    duration,
    categoryLabel,
    platform: 'RTL Play',
    channelLabel: 'RTL Play',
    resourceType: item.type ?? 'PROGRAM',
    path: `/rtlplay/program/${id}`,
    theme: inferTheme(categoryLabel, undefined, undefined, duration),
    genres: buildGenres(categoryLabel, undefined, undefined, item.tags),
    year,
    badges: item.comingSoon ? ['Bientôt'] : [],
    _raw: item,
  };
}

async function fetchRTLPlayHome(): Promise<any> {
  const res = await fetchWithRetry(
    `${RTLPLAY_BASE}/accueil?itemsPerSwimlane=20`,
    { headers: RTLPLAY_HEADERS },
    3,
    'RTL Play',
    'rtlplay_home',
    env
  );
  return res.json();
}

async function fetchRTLPlayCategory(slug: string): Promise<any[]> {
  const res = await fetchWithRetry(
    `${RTLPLAY_BASE}/${slug}?itemsPerSwimlane=50`,
    { headers: RTLPLAY_HEADERS },
    3,
    'RTL Play',
    `rtlplay_cat_${slug}`,
    env
  );
  const json = await res.json();
  // Extraire les items des rows
  return (json.rows ?? []).flatMap((row: any) => row.teasers ?? row.items ?? []);
}

// ─────────────────────────────────────────────────────────────────────────────
// NORMALISATION FRANCE.TV
// ─────────────────────────────────────────────────────────────────────────────

function normalizeFranceTVItem(item: any, categorySlug = ''): NormalizedItem | null {
  if (!item?.id) return null;
  
  const title = item.title ?? item.label ?? '';
  if (!title) return null;
  
  // Images: France.tv a une structure complexe
  let illustration: string | null = null;
  if (Array.isArray(item.images)) {
    const vignette = item.images.find((img: any) => 
      img.type === 'vignette_3x4' || img.type === 'carre'
    );
    if (vignette?.urls) {
      illustration = vignette.urls['w:400'] ?? vignette.urls['w:300'] ?? Object.values(vignette.urls)[0];
    }
  }
  
  // Durée
  const duration = item.duration 
    ?? item.playingInfos?.duration 
    ?? (item.broadcastedAt ? null : null);
  
  const categoryLabel = item.category?.label ?? categorySlug;
  
  return {
    id: `francetv-${item.id}`,
    title,
    subtitle: item.subtitle ?? item.episode_title ?? null,
    description: item.description ?? item.synopsis ?? '',
    illustration,
    duration,
    categoryLabel,
    platform: 'France.tv',
    channelLabel: item.channel_url ?? 'france.tv',
    resourceType: item.resourceType ?? (item.type === 'VIDEO' ? 'MEDIA' : 'PROGRAM'),
    path: `/francetv/${item.resourceType === 'VIDEO' ? 'video' : 'program'}/${item.id}`,
    theme: inferTheme(categoryLabel, item.typology, item.topics, duration),
    genres: buildGenres(categoryLabel, item.typology, item.topics),
    year: item.productionYear ?? undefined,
    badges: item.badges?.map((b: any) => b.label) ?? [],
    _raw: item,
  };
}

async function fetchFranceTVHome(): Promise<any> {
  const res = await fetchWithRetry(
    `${FRANCETV_BASE}/generic/homepage?platform=apps_tv`,
    { headers: FRANCETV_HEADERS },
    3,
    'France.tv',
    'francetv_home',
    env
  );
  return res.json();
}

async function fetchFranceTVCategory(slug: string): Promise<any[]> {
  const res = await fetchWithRetry(
    `${FRANCETV_BASE}/apps/categories/${slug}?platform=apps`,
    { headers: FRANCETV_HEADERS },
    3,
    'France.tv',
    `francetv_cat_${slug}`,
    env
  );
  const json = await res.json();
  return json?.data?.items ?? json?.items ?? json?.data ?? [];
}

// ─────────────────────────────────────────────────────────────────────────────
// CONSTRUCTION DES BUCKETS & RESPONSES
// ─────────────────────────────────────────────────────────────────────────────

function buildBuckets(
  rtbfItems: NormalizedItem[],
  tf1Items: NormalizedItem[],
  rtlplayItems: NormalizedItem[],
  francetvItems: NormalizedItem[]
) {
  const buckets: HomeResponse['buckets'] = [];
  
  // Regrouper par thème
  const themes: Record<ThemeKey, NormalizedItem[]> = {
    films: [], series: [], documentaire: [], info: [], sport: [],
    kids: [], divertissement: [], culture: [], musique: [], top: [],
  };
  
  [...rtbfItems, ...tf1Items, ...rtlplayItems, ...francetvItems].forEach(item => {
    if (themes[item.theme]) {
      themes[item.theme].push(item);
    }
  });
  
  // Créer les buckets
  const themeLabels: Record<ThemeKey, string> = {
    films: 'Films', series: 'Séries', documentaire: 'Documentaires',
    info: 'Info', sport: 'Sport', kids: 'Kids',
    divertissement: 'Divertissement', culture: 'Culture', musique: 'Musique', top: 'À la une',
  };
  
  (Object.keys(themes) as ThemeKey[]).forEach(theme => {
    if (themes[theme].length > 0) {
      buckets.push({
        title: themeLabels[theme],
        theme,
        items: themes[theme].slice(0, 20), // Limiter à 20 par bucket
        platform: 'Multi',
      });
    }
  });
  
  return buckets;
}

async function buildHomeData(env: Env): Promise<HomeResponse> {
  // Fetch parallèle avec fallback
  const [rtbfData, tf1Data, rtlplayData, francetvData] = await Promise.allSettled([
    safeFetch(() => fetchRTBFHome(), 'RTBF', null, env, 'rtbf_home'),
    safeFetch(() => fetchTF1Home(), 'TF1+', null, env, 'tf1_home'),
    safeFetch(() => fetchRTLPlayHome(), 'RTL Play', null, env, 'rtlplay_home'),
    safeFetch(() => fetchFranceTVHome(), 'France.tv', null, env, 'francetv_home'),
  ]);
  
  // Normalisation RTBF
  const rtbfItems: NormalizedItem[] = [];
  if (rtbfData.status === 'fulfilled' && rtbfData.value?.data?.layout?.widgets) {
    for (const widget of rtbfData.value.data.layout.widgets) {
      if (RTBF_WIDGETS[widget.id]) {
        const widgetData = await safeFetch(
          () => fetchRTBFWidget(widget.id),
          'RTBF',
          [],
          env,
          `rtbf_widget_${widget.id}`
        );
        const items = widgetData.map((item: any) => normalizeRTBFItem(item, widget.title));
        rtbfItems.push(...items.filter(Boolean));
      }
    }
  }
  
  // Normalisation TF1+
  const tf1Items: NormalizedItem[] = [];
  if (tf1Data.status === 'fulfilled' && tf1Data.value?.data?.page?.content?.sliders) {
    for (const slider of tf1Data.value.data.page.content.sliders) {
      const items = (slider.items ?? []).map((item: any) => normalizeTF1Item(item, slider.decoration?.label));
      tf1Items.push(...items.filter(Boolean));
    }
  }
  
  // Normalisation RTL Play
  const rtlplayItems: NormalizedItem[] = [];
  if (rtlplayData.status === 'fulfilled' && rtlplayData.value?.rows) {
    for (const row of rtlplayData.value.rows) {
      const items = (row.teasers ?? row.items ?? []).map((item: any) => normalizeRTLPlayItem(item, row.title));
      rtlplayItems.push(...items.filter(Boolean));
    }
  }
  
  // Normalisation France.tv
  const francetvItems: NormalizedItem[] = [];
  if (francetvData.status === 'fulfilled' && francetvData.value?.data) {
    const items = francetvData.value.data.map((item: any) => normalizeFranceTVItem(item));
    francetvItems.push(...items.filter(Boolean));
  }
  
  // Construction des buckets
  const buckets = buildBuckets(
    rtbfItems.slice(0, 50),
    tf1Items.slice(0, 50),
    rtlplayItems.slice(0, 50),
    francetvItems.slice(0, 50)
  );
  
  // Hero banners: prendre les premiers items "top" ou avec badges
  const heroBanners = [...rtbfItems, ...tf1Items, ...rtlplayItems, ...francetvItems]
    .filter(item => item.badges?.some(b => ['NEWLY_ADDED', 'LAST_CHANCE', 'EXCLUSIVE'].includes(b)) || item.theme === 'top')
    .slice(0, 5);
  
  return {
    buckets,
    heroBanners,
    meta: {
      rtbf: rtbfItems.length,
      tf1: tf1Items.length,
      rtlplay: rtlplayItems.length,
      francetv: francetvItems.length,
      buckets: buckets.length,
      builtAt: Date.now(),
    },
  };
}

async function buildListData(theme: ThemeKey, page: number, env: Env): Promise<ListResponse> {
  const items: NormalizedItem[] = [];
  const platforms: string[] = [];
  
  // RTBF
  const rtbfWidget = Object.values(RTBF_WIDGETS).find(w => w.theme === theme);
  if (rtbfWidget) {
    const widgetData = await safeFetch(
      () => fetchRTBFWidget(rtbfWidget.id),
      'RTBF',
      [],
      env,
      `rtbf_widget_${rtbfWidget.id}`
    );
    const normalized = widgetData
      .map((item: any) => normalizeRTBFItem(item, rtbfWidget.title))
      .filter(Boolean);
    items.push(...normalized);
    platforms.push('RTBF');
  }
  
  // TF1+
  const tf1Slug = TF1_CATEGORY_SLUGS[theme];
  if (tf1Slug) {
    const categoryData = await safeFetch(
      () => fetchTF1Category(tf1Slug),
      'TF1+',
      [],
      env,
      `tf1_cat_${tf1Slug}`
    );
    const normalized = categoryData
      .map((item: any) => normalizeTF1Item(item))
      .filter(Boolean);
    items.push(...normalized);
    platforms.push('TF1+');
  }
  
  // RTL Play
  const rtlSlug = RTLPLAY_SLUGS[theme];
  if (rtlSlug) {
    const categoryData = await safeFetch(
      () => fetchRTLPlayCategory(rtlSlug),
      'RTL Play',
      [],
      env,
      `rtlplay_cat_${rtlSlug}`
    );
    const normalized = categoryData
      .map((item: any) => normalizeRTLPlayItem(item))
      .filter(Boolean);
    items.push(...normalized);
    platforms.push('RTL Play');
  }
  
  // France.tv
  const ftvSlug = FRANCETV_SLUGS[theme];
  if (ftvSlug) {
    const categoryData = await safeFetch(
      () => fetchFranceTVCategory(ftvSlug),
      'France.tv',
      [],
      env,
      `francetv_cat_${ftvSlug}`
    );
    const normalized = categoryData
      .map((item: any) => normalizeFranceTVItem(item, ftvSlug))
      .filter(Boolean);
    items.push(...normalized);
    platforms.push('France.tv');
  }
  
  // Pagination
  const pageSize = 20;
  const startIndex = (page - 1) * pageSize;
  const paginatedItems = items.slice(startIndex, startIndex + pageSize);
  
  return {
    items: paginatedItems,
    meta: {
      theme,
      page,
      hasMore: startIndex + pageSize < items.length,
      platforms,
      total: items.length,
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// HANDLERS & ROUTING
// ─────────────────────────────────────────────────────────────────────────────

export interface Env {
  DATA_CACHE: KVNamespace;
  AI?: any;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    
    // CORS headers
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };
    
    // Handle preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }
    
    try {
      // ─── GET /health ─────────────────────────────────────────────────────
      if (path === '/health') {
        return new Response(JSON.stringify({ 
          status: 'ok', 
          timestamp: Date.now(),
          platforms: ['RTBF', 'TF1+', 'RTL Play', 'France.tv']
        }), {
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }
      
      // ─── GET /home ───────────────────────────────────────────────────────
      if (path === '/home') {
        // Vérifier cache
        const cached = await env.DATA_CACHE.get('home_data');
        if (cached) {
          return new Response(cached, {
            headers: { 'Content-Type': 'application/json', 'X-Cache': 'HIT', ...corsHeaders },
          });
        }
        
        // Build fresh data
        const data = await buildHomeData(env);
        
        // Cache 3 heures
        await env.DATA_CACHE.put('home_data', JSON.stringify(data), { expirationTtl: 60 * 60 * 3 });
        
        return new Response(JSON.stringify(data), {
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }
      
      // ─── GET /list?theme=films&page=1 ────────────────────────────────────
      if (path === '/list') {
        const theme = url.searchParams.get('theme') as ThemeKey;
        const page = parseInt(url.searchParams.get('page') ?? '1');
        
        if (!theme || !Object.keys(GENRE_VOCABULARY).includes(theme.replace('-', ''))) {
          return new Response(JSON.stringify({ error: 'Invalid theme' }), {
            status: 400,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
          });
        }
        
        // Cache key par thème + page
        const cacheKey = `list_${theme}_page${page}`;
        const cached = await env.DATA_CACHE.get(cacheKey);
        if (cached) {
          return new Response(cached, {
            headers: { 'Content-Type': 'application/json', 'X-Cache': 'HIT', ...corsHeaders },
          });
        }
        
        const data = await buildListData(theme, page, env);
        
        // Cache 30 minutes
        await env.DATA_CACHE.put(cacheKey, JSON.stringify(data), { expirationTtl: 60 * 30 });
        
        return new Response(JSON.stringify(data), {
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }
      
      // ─── GET /genres?theme=films ─────────────────────────────────────────
      if (path === '/genres') {
        const theme = url.searchParams.get('theme') as ThemeKey;
        const genres = Object.entries(GENRE_VOCABULARY)
          .filter(([_, t]) => inferTheme(_) === theme)
          .map(([k, v]) => v);
        
        return new Response(JSON.stringify({ theme, genres: [...new Set(genres)] }), {
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }
      
      // 404
      return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
      
    } catch (error) {
      console.error('Handler error:', error);
      return new Response(JSON.stringify({ error: 'Internal server error' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }
  },
  
  // ─── Cron job: refresh cache toutes les 3h ───────────────────────────────
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    console.log('🔄 Starting scheduled refresh...');
    
    try {
      // Pré-fetch homepage pour rafraîchir le cache
      await buildHomeData(env);
      console.log('✅ Scheduled refresh completed');
    } catch (error) {
      console.error('❌ Scheduled refresh failed:', error);
    }
  },
};
