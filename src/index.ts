/**
 * Cloudflare Worker — Aggregateur multi-plateforme StreamHub
 *
 * GET /home → { buckets, heroBanners, meta }
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
}

// ─── Map de classification statique ──────────────────────────────────────────
// Couvre 100 % des labels RTBF et TF1 observés → quasi 0 appels AI

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
  // TF1 typologies Émission → culture (talk-shows, jeux, etc.)
  'émission de flux': 'culture', 'game show': 'culture', 'quiz': 'culture',
  'talk-show': 'culture', 'variété': 'culture',

  // ── Info & Actualités ─────────────────────────────────────────────────────
  'info': 'info', 'actualité': 'info', 'actualités': 'info', 'journal': 'info',
  'politique': 'info', 'économie': 'info', 'economie': 'info',
  'news': 'info', 'débat': 'info', 'debat': 'info',
  'information': 'info',   // typology TF1 "Information"
  'actualite': 'info', 'magazine d\'info': 'info',

  // ── Téléréalité (catégorie dédiée pour TF1) ───────────────────────────────
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

// Topics TF1 → ThemeKey (quand typology est absente ou générique)
const TF1_TOPICS_MAP: Record<string, ThemeKey> = {
  // ── Téléréalité (priorité sur les autres dans le context Émission) ─────────
  'téléréalité': 'telerealite',
  'docu-réalité': 'telerealite',
  'survie': 'telerealite',
  'mariage': 'telerealite',
  'famille': 'telerealite',
  'lifestyle': 'telerealite',       // Familles nombreuses
  // NB : 'aventure' n'est PAS ici — quand typology=Film+topics=Aventure → films
  //      mais quand typology=Émission+topics=Aventure → résolu par la logique spéciale

  // ── Culture / Divertissement ──────────────────────────────────────────────
  'danse': 'culture',
  'chanson': 'culture',
  'divertissement': 'culture',
  'musique': 'culture',
  'concert': 'culture',
  'spectacle': 'culture',
  'quiz': 'culture',
  'humour': 'culture',
  'culture': 'culture',
  'talk show': 'culture',

  // ── Sport ─────────────────────────────────────────────────────────────────
  'sport': 'sport',
  'football': 'sport',
  'cyclisme': 'sport',
  'rugby': 'sport',
  'athletisme': 'sport',
  'docu-réalité sportive': 'sport',

  // ── Info ──────────────────────────────────────────────────────────────────
  'actualité': 'info',
  'journal télévisé': 'info',
  'faits divers': 'info',
  'societe': 'info',

  // ── Documentaire ─────────────────────────────────────────────────────────
  'reportages': 'documentaire',
  'enquête': 'documentaire',
  'nature': 'documentaire',
  'histoire': 'documentaire',

  // ── Thriller ─────────────────────────────────────────────────────────────
  'policier': 'thriller',
  'thriller': 'thriller',
  'suspense': 'thriller',
  'crime': 'thriller',

  // ── Films (quand topic d'un Film) ─────────────────────────────────────────
  'action': 'films',
  'aventure': 'films',
  'drame': 'films',
  'comédie': 'films',
  'romance': 'films',
  'fantastique': 'films',
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

// Buckets qui doivent être affichés en LANDSCAPE (images horizontales)
// Déterminé par la nature du contenu RTBF dans chaque bucket.
// Les items TF1 dans ces buckets utiliseront le thumbnail paysage (1280×720).
const LANDSCAPE_BUCKETS = new Set<ThemeKey>([
  'episodes', 'thriller', 'documentaire', 'culture', 'info', 'sport',
]);

// ─── Classification ───────────────────────────────────────────────────────────

function normalizeLabel(s: string): string {
  return s.toLowerCase().trim()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // strip accents
    .replace(/\s+/g, ' ');
}

function resolveTheme(
  categoryLabel: string | undefined,
  topics: string[] | undefined,
  typology: string | undefined,
  durationSec: number | undefined,
  llmCache: Record<string, ThemeKey>,
): ThemeKey {
  // Typologies "génériques" TF1 qui ne doivent PAS court-circuiter
  // le check des topics (Émission peut être téléréalité, culture, etc.)
  const GENERIC_TYPOLOGIES = new Set(['emission', 'spectacle']);

  // 1. Topics en PREMIER pour les typologies génériques TF1
  //    → ex: Émission + topics["Téléréalité","Mariage"] → telerealite
  //         Émission + topics["Aventure","Action"] → telerealite (Koh-Lanta)
  //         Émission + topics["Divertissement","Danse"] → culture
  //    (pour Film, Sport, Information : typology précise → traitement normal au step 2)
  const TELEREALITE_TOPICS = new Set([
    'telerealite', 'docu-realite', 'mariage', 'famille', 'lifestyle',
    'survie', 'aventure',   // Koh-Lanta, Survivor, etc.
  ]);

  if (typology && GENERIC_TYPOLOGIES.has(normalizeLabel(typology)) && topics?.length) {
    // D'abord vérifier les topics téléréalité (priorité absolue)
    for (const topic of topics) {
      const t = normalizeLabel(topic);
      if (TELEREALITE_TOPICS.has(t)) return 'telerealite';
    }
    // Ensuite les autres topics
    for (const topic of topics) {
      const t = normalizeLabel(topic);
      if (TF1_TOPICS_MAP[t] && TF1_TOPICS_MAP[t] !== 'films') return TF1_TOPICS_MAP[t];
      if (CATEGORY_MAP[t] && CATEGORY_MAP[t] !== 'culture' && CATEGORY_MAP[t] !== 'films') return CATEGORY_MAP[t];
    }
    // Si que des topics "films" ou "culture", retomber sur culture pour une Émission
    for (const topic of topics) {
      const t = normalizeLabel(topic);
      if (TF1_TOPICS_MAP[t]) return TF1_TOPICS_MAP[t];
    }
    // Fallback: Émission générique → culture
    return 'culture';
  }

  // 2. Typology précise (Film, Sport, Information…)
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

  // 3. CategoryLabel exact puis fuzzy
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

  // 4. Topics (cas général : pas de typology générique traitée avant)
  if (topics?.length) {
    for (const topic of topics) {
      const t = normalizeLabel(topic);
      if (TF1_TOPICS_MAP[t]) return TF1_TOPICS_MAP[t];
      if (CATEGORY_MAP[t]) return CATEGORY_MAP[t];
    }
  }

  // 5. Fallback
  return 'series';
}

// ─── Workers AI (appelé uniquement pour les labels inconnus résiduels) ────────

// Dictionnaire de secours local pour les labels fréquemment inconnus
// Évite d'appeler l'AI pour des cas déjà vus
const LOCAL_FALLBACK_MAP: Record<string, ThemeKey> = {
  // Labels RTBF fréquents non couverts
  'conte': 'kids', 'fable': 'kids', 'marionnettes': 'kids',
  'short': 'films', 'court metrage': 'films',
  'catchup': 'culture', 'replay': 'culture',
  'emission sportive': 'sport', 'magazine sportif': 'sport',
  'actu': 'info', 'flash info': 'info', 'meteo': 'info',
  'sante': 'documentaire', 'medical': 'documentaire',
  'cuisine': 'documentaire', 'gastronomie': 'documentaire',
  'mode': 'culture', 'deco': 'culture',
  'talkshow': 'culture', 'late show': 'culture',
  // Labels TF1 fréquents
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

  // 1. Essayer de résoudre localement d'abord
  const resolved: Record<string, ThemeKey> = {};
  const stillUnknown: string[] = [];

  for (const label of unknownLabels) {
    const k = normalizeLabel(label);
    if (LOCAL_FALLBACK_MAP[k]) {
      resolved[k] = LOCAL_FALLBACK_MAP[k];
    } else {
      // Fuzzy sur le fallback map
      let found: ThemeKey | null = null;
      for (const [frag, theme] of Object.entries(LOCAL_FALLBACK_MAP)) {
        if (k.includes(frag) || frag.includes(k)) { found = theme; break; }
      }
      if (found) resolved[k] = found;
      else stillUnknown.push(label);
    }
  }

  if (stillUnknown.length === 0) {
    console.log('[AI] Tous les labels résolus localement:', resolved);
    return resolved;
  }

  // 2. Appeler l'AI uniquement pour ce qui reste vraiment inconnu
  const themeKeys = Object.keys(THEMES).join(', ');
  const prompt = `Tu es un classificateur de genres vidéo.
Pour chaque label ci-dessous, retourne le thème le plus proche parmi : ${themeKeys}.
Réponds UNIQUEMENT avec un objet JSON valide, sans texte autour.
Format : { "label": "theme", ... }

Labels :
${stillUnknown.map(l => `- "${l}"`).join('\n')}`;

  try {
    console.log('[AI] Classification de', stillUnknown.length, 'labels:', stillUnknown);
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
  } catch (err) {
    console.error('[worker] AI classification failed, using series fallback for:', stillUnknown);
    // En cas d'échec AI : fallback 'series' pour tout le reste
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
      `${url}${url.includes('?') ? '&' : '?'}_limit=24&_embed=content`,
      { headers: { Accept: 'application/json' } },
    );
    if (!res.ok) return [];
    const json = await res.json();
    return json?.data?.content ?? json?.data ?? [];
  } catch {
    return [];
  }
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

// ─── Normalisation RTBF ───────────────────────────────────────────────────────

function normalizeRTBFItem(item: any, llmCache: Record<string, ThemeKey>, widgetTitle = ''): NormalizedItem | null {
  if (!item || item.resourceType === 'LIVE') return null;

  // Si le widget source s'appelle "Kids" → forcer le thème kids
  // (les items PROGRAM_LIST Kids n'ont pas de categoryLabel)
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

  // ── Identité ────────────────────────────────────────────────────────────────
  const id    = prog.id ?? item.id;
  const title = prog.decoration?.label ?? prog.name ?? item.decoration?.label ?? item.name ?? item.label ?? '';
  if (!title || !id) return null;

  const typology    = prog.typology ?? item.typology ?? '';
  const topics: string[] = prog.topics ?? item.program?.topics ?? [];
  const duration    = item.duration ?? prog.duration ?? 0;
  const rawCategory = typology || (item.__typename === 'Video' ? 'Divertissement' : '');

  // ── Classification ──────────────────────────────────────────────────────────
  // TopProgramItem → bucket dédié 'top' (classement TF1)
  const isTopProgram = item.__typename === 'TopProgramItem';
  const theme: ThemeKey = isTopProgram
    ? 'top'
    : resolveTheme(rawCategory || undefined, topics, typology || undefined, duration, llmCache);

  // ── Images ──────────────────────────────────────────────────────────────────
  // Règle : le ratio de l'image TF1 suit le bucket de destination.
  //
  //  Bucket portrait  (films, series, top, telerealite…)
  //    → portrait 217×289  → VideoCard détecte /217/289/ → aspect-[2/3] ✓
  //
  //  Bucket landscape (episodes, thriller, documentaire, culture, info, sport)
  //    → thumbnail 1280×720 → VideoCard détecte /1280/720/ → aspect-video ✓

  const portraitUrl =
    pickBestUrl(prog.decoration?.portrait?.sourcesWithScales) ??
    pickBestUrl(item.decoration?.portrait?.sourcesWithScales) ??
    pickBestUrl(item.thumbnail?.sourcesWithScales);

  const landscapeUrl =
    pickBestUrl(prog.decoration?.thumbnail?.sourcesWithScales) ??
    pickBestUrl(item.decoration?.thumbnail?.sourcesWithScales) ??
    pickBestUrl(item.image?.sourcesWithScales);

  // Choix du ratio selon le bucket de destination
  const useLandscape = LANDSCAPE_BUCKETS.has(theme);
  const primaryUrl = useLandscape
    ? (landscapeUrl ?? portraitUrl)   // bucket landscape → paysage en priorité
    : (portraitUrl  ?? landscapeUrl); // bucket portrait  → portrait en priorité

  // Toutes les clés = primaryUrl → VideoCard détecte le bon ratio uniformément
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

  // Pour les films TF1 (CoverOfProgram avec typology=Film), l'ID de la vidéo
  // est dans le premier CTA WatchButtonAction, pas dans prog.id
  const ctaVideoId = item.callToAction?.items?.find(
    (i: any) => i.type === 'PLAY' || i.__typename === 'WatchButtonAction'
  )?.video?.id ?? null;

  const mediaId  = isVideo ? id : (isFilm ? (ctaVideoId ?? id) : undefined);
  const streamId = mediaId;
  const assetId  = mediaId;

  // ── Badges & métadonnées ────────────────────────────────────────────────────
  // VideoCard attend : stamp, hasSubtitles, hasAudioDescriptions, rating, publishedTo
  const hasSubtitles       = (prog.hasFrenchDeafSubtitles?.total ?? 0) > 0
                           || (prog.hasFrenchSubtitles?.total ?? 0) > 0;
  const hasAudioDescriptions = (prog.hasDescriptionTrack?.total ?? 0) > 0;

  let rating: string | null = prog.rating ?? item.rating ?? null;
  if (rating) rating = rating.replace('CSA_', '').replace('ALL', 'Tout public');

  // Stamp depuis badges TF1 (ex: "Nouvel épisode", "Avant-première"…)
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
    // Badges et métadonnées visibles dans VideoCard
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

// ─── Build buckets : interleave RTBF + TF1 ───────────────────────────────────
// Pour éviter que RTBF remplisse le bucket avant TF1, on intercale les deux.

function buildBuckets(
  rtbfItems: NormalizedItem[],
  tf1Items: NormalizedItem[],
): ThematicBucket[] {
  // Regrouper par thème séparément
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

      // Interleave : 1 RTBF, 1 TF1, 1 RTBF, 1 TF1 … sans limite
      // Le frontend scroll horizontalement, pas besoin de couper
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
      };
    })
    .filter(b => b.items.length > 0);
}

// ─── Banners helpers ──────────────────────────────────────────────────────────

function buildRTBFBanners(rtbfHome: any, promoboxItems: any[] = []): any[] {
  const banners: any[] = [];

  // Priorité aux items pré-fetchés, sinon tenter w.data?.content (rarement présent)
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

    // L'API RTBF retourne image: { xs, s, m, l, xl } directement (URLs absolues)
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
    // Structure réelle : cover.decoration contient label, description, cover, video
    // cover.program contient id, slug, typology, topics, etc.
    // cover.video (CoverOfVideo) contient id, slug, season, episode, playingInfos.duration
    const deco = cover.decoration ?? {};
    const prog = cover.program ?? {};
    // Pour CoverOfVideo, récupérer les infos depuis cover.video
    const vid  = cover.video  ?? {};

    const id    = cover.id ?? prog.id ?? vid.id;
    const title = deco.label ?? prog.name ?? vid.program?.name ?? '';
    if (!id || !title) continue;

    const desc = deco.description ?? deco.catchPhrase ?? deco.summary ?? '';

    // ── Image ────────────────────────────────────────────────────────────────
    // decoration.cover → image paysage (640×360 base, scale 2x/3x disponible)
    // decoration.coverSmall → portrait (430×660)
    // On préfère cover (paysage) pour xl/l/m et coverSmall pour xs/s
    const bgSrcs   = [...(deco.cover?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 1) - (a.scale ?? 1));
    const portSrcs = [...(deco.coverSmall?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 1) - (a.scale ?? 1));

    // Choisir la meilleure URL selon la résolution disponible
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

    // ── Vidéo bande-annonce ───────────────────────────────────────────────────
    // decoration.video.sources[0].url
    const videoUrl = deco.video?.sources?.[0]?.url ?? null;

    // ── ContentType / IDs ─────────────────────────────────────────────────────
    // CoverOfProgram → contentType=program, id=prog.id, slug=prog.slug
    //                  Le 1er CTA WatchButtonAction donne l'id de la vidéo à jouer
    // CoverOfVideo   → contentType=video ou media, id=vid.id, slug=vid.slug
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
      // programId/programSlug déjà assignés depuis prog
    } else {
      // CoverOfProgram ou fallback
      contentType = 'program';
      // Le 1er CTA de type PLAY donne la vidéo à jouer
      const ctaPlay = cover.callToAction?.items?.find((i: any) => i.type === 'PLAY' || i.__typename === 'WatchButtonAction');
      contentId    = ctaPlay?.video?.id ?? ctaPlay?.id ?? prog.id ?? id;
      contentSlug  = ctaPlay?.video?.slug ?? prog.slug ?? cover.contentSlug ?? null;
    }

    // ── Métadonnées ───────────────────────────────────────────────────────────
    // Priorité : CoverOfVideo.video > CoverOfProgram.callToAction.video
    const ctaVideo = cover.callToAction?.items?.find((i: any) => i.video)?.video ?? {};
    const season   = vid.season   ?? ctaVideo?.season   ?? null;
    const episode  = vid.episode  ?? ctaVideo?.episode  ?? null;
    const duration = vid.playingInfos?.duration ?? ctaVideo?.playingInfos?.duration ?? null;
    const rights   = vid.rights   ?? ctaVideo?.rights   ?? cover.rights ?? [];

    // Topics depuis le programme associé (CoverOfVideo: vid.program, CoverOfProgram: prog)
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

  // Fallback : prend les 5 premiers items du premier slider
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

    if (url.pathname !== '/home') {
      return new Response('Not found', { status: 404, headers: corsHeaders });
    }

    try {
      // ── 1. Fetch RTBF + TF1 en parallèle ──────────────────────────────────
      const [rtbfResult, tf1Result] = await Promise.allSettled([
        fetchRTBF(),
        fetchTF1(),
      ]);

      const rtbfHome = rtbfResult.status === 'fulfilled' ? rtbfResult.value : null;
      const tf1Raw   = tf1Result.status  === 'fulfilled' ? tf1Result.value  : null;

      if (rtbfResult.status === 'rejected') console.error('[worker] RTBF fetch error:', rtbfResult.reason);
      if (tf1Result.status  === 'rejected') console.error('[worker] TF1 fetch error:',  tf1Result.reason);

      // ── 2. Banners hero ────────────────────────────────────────────────────
      // Pour RTBF, le PROMOBOX doit être fetché séparément (widget contentPath)
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

      // ── 3. Lire le cache LLM une seule fois ────────────────────────────────
      const llmCacheRaw = await env.LABEL_CACHE.get('label_map').catch(() => null);
      const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

      // ── 4. Normaliser items RTBF ───────────────────────────────────────────
      let rtbfItems: NormalizedItem[] = [];
      if (rtbfHome) {
        // EXCLUDED : widgets qui n'ont pas d'items à afficher comme cartes
        // PROMOBOX → géré par buildRTBFBanners (banners hero)
        // BANNER   → plus utilisé par RTBF (remplacé par PROMOBOX)
        const EXCLUDED = new Set([
          'FAVORITE_PROGRAM_LIST', 'CHANNEL_LIST', 'ONGOING_PLAY_HISTORY',
          'CATEGORY_LIST', 'BANNER', 'MEDIA_TRAILER',
          'PROMOBOX', // Géré séparément dans buildRTBFBanners
        ]);
        const widgets = rtbfHome.data?.widgets ?? [];

        // Construire les fetches avec le titre du widget (pour Kids detection)
        const widgetMetas = widgets
          .filter((w: any) => !EXCLUDED.has(w.type) && w.contentPath)
          .map((w: any) => ({ title: w.title ?? '', fetch: fetchRTBFWidget(w.contentPath) }));

        const fetches     = widgetMetas.map((m: any) => m.fetch);
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

      // ── 5. Normaliser items TF1 ────────────────────────────────────────────
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

      // ── 6. Déduplication par plateforme ───────────────────────────────────
      rtbfItems = deduplicate(rtbfItems);
      tf1Items  = deduplicate(tf1Items);

      // ── 7. AI : uniquement pour les labels résiduels inconnus ─────────────
      // On ne classe pas comme "inconnu" un item qui a un thème != 'series'
      // ou qui a une typology/topic reconnue. L'AI ne traite que les labels
      // catégorisés 'series' dont la catégorie brute est inconnue.
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

          // Reclassifier les items affectés
          for (const item of allForAI) {
            if (item.theme === 'series' && item.categoryLabel) {
              const k = normalizeLabel(item.categoryLabel);
              if (newMappings[k]) item.theme = newMappings[k];
            }
          }
          // Re-split après mise à jour
          rtbfItems = allForAI.filter(i => i.platform === 'RTBF');
          tf1Items  = allForAI.filter(i => i.platform === 'TF1+');
        }
      }

      // ── 8. Construire les buckets (interleaved RTBF + TF1) ────────────────
      const buckets = buildBuckets(rtbfItems, tf1Items);

      // ── 9. Répondre ────────────────────────────────────────────────────────
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
