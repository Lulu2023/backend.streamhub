/**
 * Cloudflare Worker — Aggregateur multi-plateforme
 *
 * Déploiement :
 *   npm install -g wrangler
 *   wrangler login
 *   wrangler deploy
 *
 * Une seule requête depuis le téléphone :
 *   GET /home  →  { buckets: ThematicBucket[] }
 *
 * Le Worker fait les requêtes RTBF + TF1 en parallèle côté serveur,
 * classifie les labels inconnus via Cloudflare Workers AI (gratuit),
 * et renvoie des buckets prêts.
 *
 * Bindings à ajouter dans le dashboard Cloudflare (Workers & Pages → ton worker → Settings) :
 *   - KV Namespace : LABEL_CACHE  (créer via Storage & Databases → KV)
 *   - Workers AI   : AI           (activer via AI → Workers AI → Enable)
 */

export interface Env {
  // Workers AI — binding natif Cloudflare, gratuit jusqu'à 10 000 req/jour
  AI: Ai;
  // KV namespace pour le cache des labels classifiés
  LABEL_CACHE: KVNamespace;
}

// ─── Types ────────────────────────────────────────────────────────────────────

type ThemeKey = 'thriller' | 'films' | 'documentaire' | 'culture' | 'info' | 'kids' | 'sport' | 'series';

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

// ─── Constantes ───────────────────────────────────────────────────────────────

const CATEGORY_MAP: Record<string, ThemeKey> = {
  // ── Policier / Thriller ──────────────────────────────────────────────────
  'policier': 'thriller', 'affaires criminelles': 'thriller', 'crime': 'thriller',
  'thriller': 'thriller', 'serie policiere': 'thriller', 'série policière': 'thriller',
  'police': 'thriller', 'suspense': 'thriller', 'horreur': 'thriller',
  'espionnage': 'thriller',

  // ── Films ────────────────────────────────────────────────────────────────
  // NB : "drame" et "comédie dramatique" sont traités via heuristique durée
  'film': 'films', 'films': 'films',
  'comédie': 'films', 'comedie': 'films',
  'comédie dramatique': 'films', 'comedie dramatique': 'films',
  'action': 'films', 'aventure': 'films',
  'science-fiction': 'films', 'sf': 'films', 'fantastique': 'films',
  'animation': 'films', 'romance': 'films', 'western': 'films',
  'biopic': 'films', 'téléfilm': 'films', 'telefilm': 'films',
  'drame': 'films', 'cinema': 'films',

  // ── Documentaire ─────────────────────────────────────────────────────────
  'documentaire': 'documentaire', 'investigation': 'documentaire',
  'société': 'documentaire', 'societe': 'documentaire',
  'histoire': 'documentaire', 'découvertes': 'documentaire', 'decouvertes': 'documentaire',
  'reportage': 'documentaire', 'nature': 'documentaire', 'science': 'documentaire',
  'environnement': 'documentaire', 'voyage': 'documentaire',
  'monde': 'documentaire',

  // ── Culture & Divertissement ──────────────────────────────────────────────
  'culture': 'culture', 'divertissement': 'culture', 'humour': 'culture',
  'musique': 'culture', 'talk show': 'culture', 'variétés': 'culture',
  'varietes': 'culture', 'magazine': 'culture', 'lifestyle': 'culture',
  'spectacle': 'culture', 'concert': 'culture',
  'people & musique': 'culture', 'people': 'culture',
  'litterature': 'culture', 'littérature': 'culture',
  'jeux': 'culture',  // jeux TV (Billets doux, Génies en herbe) = divertissement adulte
  'jeux & divertissements': 'culture',
  'émission': 'culture', 'emission': 'culture',  // émission TV générique

  // ── Info & Actualités ─────────────────────────────────────────────────────
  'info': 'info', 'actualité': 'info', 'actualités': 'info', 'journal': 'info',
  'politique': 'info', 'économie': 'info', 'economie': 'info',
  'news': 'info', 'débat': 'info', 'debat': 'info',
  'information': 'info',  // typology TF1

  // ── Kids ──────────────────────────────────────────────────────────────────
  'kids': 'kids', 'enfants': 'kids', 'jeunesse': 'kids',
  'animé': 'kids', 'anime': 'kids', 'dessin animé': 'kids',

  // ── Sport ─────────────────────────────────────────────────────────────────
  'sport': 'sport', 'football': 'sport', 'cyclisme': 'sport', 'tennis': 'sport',
  'rugby': 'sport', 'formule 1': 'sport', 'athlétisme': 'sport',
  'moteurs': 'sport', 'basket': 'sport', 'natation': 'sport',

  // ── Séries (fallback) ─────────────────────────────────────────────────────
  'serie': 'series', 'série': 'series', 'sitcom': 'series', 'feuilleton': 'series',
  'mini serie': 'series', 'mini série': 'series',
};

const THEMES: Record<ThemeKey, { label: string; emoji: string }> = {
  thriller:     { label: 'Policier & Thriller',      emoji: '🔍' },
  films:        { label: 'Films',                    emoji: '🎬' },
  documentaire: { label: 'Documentaires',            emoji: '📽️' },
  culture:      { label: 'Culture & Divertissement', emoji: '🎭' },
  info:         { label: 'Info & Actualités',        emoji: '📰' },
  series:       { label: 'Séries',                   emoji: '📺' },
  kids:         { label: 'Kids',                     emoji: '🌟' },
  sport:        { label: 'Sport',                    emoji: '⚽' },
};

const BUCKET_ORDER: ThemeKey[] = ['thriller', 'films', 'series', 'documentaire', 'culture', 'info', 'sport', 'kids'];

// ─── Classification ───────────────────────────────────────────────────────────

function resolveThemeSync(categoryLabel: string | undefined, durationSec: number | undefined, llmCache: Record<string, ThemeKey>): ThemeKey {
  if (!categoryLabel) return 'series';
  const key = categoryLabel.toLowerCase().trim();

  // 1. Lookup exact
  const mapped = CATEGORY_MAP[key];
  if (mapped) {
    // Heuristique durée : drame/comédie >80min → film, sinon série
    const isDramaOrComedy = ['drame', 'comédie', 'comedie', 'comédie dramatique', 'comedie dramatique'].includes(key);
    if (isDramaOrComedy && durationSec !== undefined) {
      return durationSec > 4800 ? 'films' : 'series';
    }
    return mapped;
  }

  // 2. Fuzzy : le label COMMENCE PAR un fragment connu (évite les faux positifs)
  //    Ex : "comédie dramatique" commence par "comédie" ✓
  //    Mais "spectacle" ne commence pas par "sport" ✓ (pas de faux positif)
  for (const [fragment, theme] of Object.entries(CATEGORY_MAP)) {
    if (key.startsWith(fragment)) return theme;
  }

  // 3. Fuzzy élargi : contient le fragment (seulement pour les fragments longs ≥6 chars)
  for (const [fragment, theme] of Object.entries(CATEGORY_MAP)) {
    if (fragment.length >= 6 && key.includes(fragment)) return theme;
  }

  // 4. Cache AI
  if (llmCache[key]) return llmCache[key];

  // 5. Fallback — ce label sera envoyé à Workers AI
  return 'series';
}

/**
 * Envoie les labels inconnus à Ollama en un seul appel.
 * Retourne un map label → ThemeKey.
 */
async function classifyWithWorkersAI(
  unknownLabels: string[],
  env: Env,
): Promise<Record<string, ThemeKey>> {
  if (unknownLabels.length === 0) return {};

  const themeKeys = Object.keys(THEMES).join(', ');
  const prompt = `Tu es un classificateur de genres vidéo.
Pour chaque label ci-dessous, retourne le thème le plus proche parmi : ${themeKeys}.
Réponds UNIQUEMENT avec un objet JSON valide, sans texte autour.
Format : { "label": "theme", ... }

Labels :
${unknownLabels.map(l => `- "${l}"`).join('\n')}`;

  try {
    // Workers AI — Llama 3 8B, gratuit jusqu'à 10 000 req/jour
    console.log('[AI] Classification de', unknownLabels.length, 'labels:', unknownLabels);
    const response = await env.AI.run('@cf/meta/llama-3-8b-instruct', {
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 512,
    });

    const text = (response as any).response ?? '';
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Réponse Workers AI non parseable');

    const result: Record<string, string> = JSON.parse(jsonMatch[0]);
    const valid: Record<string, ThemeKey> = {};

    for (const [label, theme] of Object.entries(result)) {
      if (Object.keys(THEMES).includes(theme)) {
        valid[label.toLowerCase().trim()] = theme as ThemeKey;
      }
    }

    return valid;
  } catch (err) {
    console.error('[worker] Workers AI classification failed:', err);
    return {};
  }
}

// ─── Fetch plateformes ────────────────────────────────────────────────────────

async function fetchRTBF(): Promise<any> {
  const url = 'https://bff-service.rtbf.be/auvio/v1.23/pages/home?userAgent=Chrome-web-3.0';
  const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
  if (!res.ok) throw new Error(`RTBF ${res.status}`);
  return res.json();
}

async function fetchRTBFWidget(contentPath: string): Promise<any[]> {
  try {
    const url = contentPath.startsWith('http') ? contentPath : `https://bff-service.rtbf.be${contentPath}`;
    const res = await fetch(`${url}${url.includes('?') ? '&' : '?'}_limit=20&_embed=content`, {
      headers: { 'Accept': 'application/json' },
    });
    if (!res.ok) return [];
    const json = await res.json();
    return json?.data?.content ?? json?.data ?? [];
  } catch {
    return [];
  }
}

async function fetchTF1(): Promise<any> {
  const headers = {
    'content-type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Origin': 'https://www.tf1.fr',
    'Referer': 'https://www.tf1.fr/',
  };

  // Requête homepage complète (tous types de contenus + banners LARGE/MEDIUM)
  const homeVars = {
    ofBannerTypes: ['LARGE', 'MEDIUM'],
    ofContentTypes: [
      'ARTICLE', 'CATEGORY', 'CHANNEL', 'COLLECTION', 'EXTERNAL_LINK', 'LANDING_PAGE',
      'LIVE', 'NEXT_BROADCAST', 'PERSONALITY', 'PLAYLIST', 'PLUGIN', 'PROGRAM',
      'PROGRAM_BY_CATEGORY', 'SMART_SUMMARY', 'TOP_PROGRAM', 'TOP_VIDEO', 'TRAILER', 'VIDEO',
    ],
    ofChannelTypes: ['CORNER', 'DIGITAL', 'EVENT', 'PARTNER', 'TV'],
  };

  // Requête banners hero (homeCoversByRight)
  const [homeRes, bannersRes] = await Promise.allSettled([
    fetch(
      `https://www.tf1.fr/graphql/fr-be/web?id=c34093152db844db6b7ad9b56df12841f7d13182&variables=${encodeURIComponent(JSON.stringify(homeVars))}`,
      { method: 'GET', headers }
    ),
    fetch(
      `https://www.tf1.fr/graphql/web?id=bd8e6aab9996844dad4ea9a53887adad27d86151`,
      { method: 'GET', headers }
    ),
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

// ─── Normalisation ────────────────────────────────────────────────────────────

function normalizeRTBFItem(item: any, llmCache: Record<string, ThemeKey>): NormalizedItem | null {
  if (!item || item.resourceType === 'LIVE') return null;
  const theme = resolveThemeSync(item.categoryLabel, item.duration, llmCache);
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

function normalizeTF1Item(item: any, llmCache: Record<string, ThemeKey>): NormalizedItem | null {
  if (!item) return null;

  const isFilm = item.typology === 'Film' || item.__typename === 'Video' && item.genre === 'Film';
  const rawCategory =
    item.typology
    ?? item.genre
    ?? item.program?.typology
    ?? item.program?.genre
    ?? (isFilm ? 'Film' : undefined)
    ?? (item.__typename === 'Video' ? 'Divertissement' : 'Série');

  const theme = resolveThemeSync(rawCategory, item.duration, llmCache);

  // Image
  const sourcesWithScales =
    item.image?.sourcesWithScales
    ?? item.decoration?.portrait?.sourcesWithScales
    ?? item.program?.decoration?.portrait?.sourcesWithScales
    ?? [];
  const bestUrl = sourcesWithScales.sort((a: any, b: any) => (b.scale ?? 0) - (a.scale ?? 0))[0]?.url;
  const illustration = bestUrl ? { xs: bestUrl, s: bestUrl, m: bestUrl, l: bestUrl, xl: bestUrl } : undefined;

  const id = item.id ?? item.program?.id;
  const title = item.decoration?.label ?? item.title ?? item.program?.decoration?.label ?? '';
  if (!title || !id) return null;

  return {
    id: `tf1-${id}`,
    title,
    subtitle: item.decoration?.catchPhrase ?? item.program?.decoration?.catchPhrase,
    description: item.synopsis ?? item.program?.synopsis ?? item.program?.decoration?.description,
    illustration,
    duration: item.duration ?? 0,
    categoryLabel: rawCategory,
    platform: 'TF1+',
    channelLabel: 'TF1+',
    resourceType: item.__typename === 'Video' ? 'MEDIA' : 'PROGRAM',
    path: `/tf1/${item.__typename === 'Video' ? 'video' : 'program'}/${id}`,
    theme,
    _raw: item,
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

function buildBuckets(items: NormalizedItem[], maxPerBucket = 20): ThematicBucket[] {
  const groups = new Map<ThemeKey, NormalizedItem[]>();
  for (const theme of BUCKET_ORDER) groups.set(theme, []);

  for (const item of items) {
    groups.get(item.theme)?.push(item);
  }

  return BUCKET_ORDER
    .map(theme => ({
      theme,
      label: THEMES[theme].label,
      emoji: THEMES[theme].emoji,
      items: (groups.get(theme) ?? []).slice(0, maxPerBucket),
    }))
    .filter(b => b.items.length > 0);
}

// ─── Handler principal ────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // CORS pour le téléphone
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
      // 1. Fetch RTBF + TF1 en parallèle
      const [rtbfHome, tf1Raw] = await Promise.allSettled([
        fetchRTBF(),
        fetchTF1(),
      ]);

      // 2. Extraire les banners RTBF + highlights TF1 (hero carousel)
      const heroBanners: any[] = [];
      if (rtbfHome.status === 'fulfilled') {
        const allWidgets = rtbfHome.value?.data?.widgets ?? [];
        for (const w of allWidgets) {
          if (w.type === 'BANNER' && w.data) {
            heroBanners.push({
              id: w.data.id ?? w.id ?? String(Math.random()),
              title: w.data.title ?? '',
              description: w.data.description ?? '',
              image: w.data.image ?? null,
              deepLink: w.data.deepLink ?? null,
              backgroundColor: w.data.backgroundColor ?? '#000000',
              platform: 'RTBF',
            });
          }
        }
      }
      if (tf1Raw.status === 'fulfilled') {
        // homeCoversByRight = tableau plat de covers hero TF1
        const covers: any[] = tf1Raw.value?._tf1Banners ?? [];
        for (const cover of covers.slice(0, 6)) {
          const id    = cover.id ?? cover.program?.id;
          const title = cover.title ?? cover.program?.decoration?.label ?? cover.program?.name ?? '';
          const desc  = cover.description ?? cover.program?.decoration?.catchPhrase ?? '';
          const imgUrl = cover.image?.url
            ?? [...(cover.image?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 0) - (a.scale ?? 0))[0]?.url;
          if (id && title) {
            heroBanners.push({
              id: `tf1-banner-${id}`,
              title,
              description: desc,
              image: imgUrl ? { xs: imgUrl, s: imgUrl, m: imgUrl, l: imgUrl, xl: imgUrl } : null,
              deepLink: cover.link ?? (cover.program?.slug ? `/tf1/program/${cover.program.slug}` : null),
              backgroundColor: '#000000',
              platform: 'TF1+',
            });
          }
        }
        // Fallback si homeCoversByRight vide : premier slider homepage
        if (covers.length === 0) {
          const sliders: any[] = tf1Raw.value?.data?.homeSliders ?? [];
          for (const item of (sliders[0]?.items ?? []).slice(0, 5)) {
            const prog  = item.program ?? item;
            const id    = prog.id ?? item.id;
            const title = prog.decoration?.label ?? prog.name ?? '';
            const desc  = prog.decoration?.catchPhrase ?? prog.decoration?.description ?? '';
            const sources = [
              ...(prog.decoration?.thumbnail?.sourcesWithScales ?? []),
              ...(prog.decoration?.portrait?.sourcesWithScales  ?? []),
              ...(item.image?.sourcesWithScales ?? []),
            ].sort((a: any, b: any) => (b.scale ?? 0) - (a.scale ?? 0));
            const bestImg = sources[0]?.url;
            if (id && title) {
              heroBanners.push({
                id: `tf1-banner-${id}`,
                title,
                description: desc,
                image: bestImg ? { xs: bestImg, s: bestImg, m: bestImg, l: bestImg, xl: bestImg } : null,
                deepLink: `/tf1/program/${id}`,
                backgroundColor: '#000000',
                platform: 'TF1+',
              });
            }
          }
        }
      }

      // 3. Charger les widgets RTBF (contentPath → items)
      let rtbfItems: NormalizedItem[] = [];
      if (rtbfHome.status === 'fulfilled') {
        const EXCLUDED = new Set(['FAVORITE_PROGRAM_LIST', 'CHANNEL_LIST', 'ONGOING_PLAY_HISTORY', 'CATEGORY_LIST', 'PROMOBOX', 'BANNER', 'MEDIA_TRAILER']);
        const widgets = rtbfHome.value?.data?.widgets ?? [];
        const widgetFetches = widgets
          .filter((w: any) => !EXCLUDED.has(w.type) && w.contentPath)
          .map((w: any) => fetchRTBFWidget(w.contentPath));

        const widgetResults = await Promise.allSettled(widgetFetches);

        // Lire le cache LLM depuis KV
        const llmCacheRaw = await env.LABEL_CACHE.get('label_map');
        const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

        for (const result of widgetResults) {
          if (result.status !== 'fulfilled') continue;
          for (const raw of result.value) {
            const item = normalizeRTBFItem(raw, llmCache);
            if (item) rtbfItems.push(item);
          }
        }
      }

      // 3. Normaliser TF1
      let tf1Items: NormalizedItem[] = [];
      const llmCacheRaw = await env.LABEL_CACHE.get('label_map');
      const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

      if (tf1Raw.status === 'fulfilled') {
        // La persisted query retourne data.homeSliders (même structure que transformHomePageData)
        const sliders = tf1Raw.value?.data?.homeSliders ?? [];
        for (const slider of sliders) {
          for (const item of slider.items ?? []) {
            // Extraire le programme selon le __typename (même logique que transformSliderItem)
            const prog = item.program ?? item; // TopProgramItem/ProgramItem ont un sous-objet program
            const id = prog.id ?? item.id;
            const typology = prog.typology ?? item.typology ?? '';
            const genre = prog.genre ?? item.genre ?? '';
            const title = prog.decoration?.label ?? prog.name ?? item.decoration?.label ?? '';
            if (!title || !id) continue;

            const sourcesWithScales =
              prog.decoration?.portrait?.sourcesWithScales ??
              prog.decoration?.image?.sourcesWithScales ??
              item.image?.sourcesWithScales ?? [];
            const bestUrl = [...sourcesWithScales].sort((a: any, b: any) => (b.scale ?? 0) - (a.scale ?? 0))[0]?.url;
            const illustration = bestUrl ? { xs: bestUrl, s: bestUrl, m: bestUrl, l: bestUrl, xl: bestUrl } : undefined;

            const rawCategory = typology || genre || (item.__typename === 'Video' ? 'Divertissement' : 'Série');
            const theme = resolveThemeSync(rawCategory, item.duration ?? 0, llmCache);

            tf1Items.push({
              id: `tf1-${id}`,
              title,
              subtitle: prog.decoration?.catchPhrase,
              description: prog.synopsis ?? prog.decoration?.description,
              illustration,
              duration: item.duration ?? 0,
              categoryLabel: rawCategory,
              platform: 'TF1+',
              channelLabel: 'TF1+',
              resourceType: item.__typename === 'Video' ? 'MEDIA' : 'PROGRAM',
              path: `/tf1/${item.__typename === 'Video' ? 'video' : 'program'}/${id}`,
              theme,
              _raw: item,
            });
          }
        }
      }

      // 4. Trouver les labels inconnus et les classifier avec Workers AI
      const allItems = deduplicate([...rtbfItems, ...tf1Items]);
      const unknownLabels = [...new Set(
        allItems
          .filter(i => i.theme === 'series' && i.categoryLabel)
          .map(i => i.categoryLabel!.toLowerCase().trim())
          .filter(l => !CATEGORY_MAP[l] && !llmCache[l])
      )];

      let newMappings: Record<string, ThemeKey> = {};

      if (unknownLabels.length > 0) {
        console.log('[worker] Labels inconnus envoyés à Workers AI:', unknownLabels);
        newMappings = await classifyWithWorkersAI(unknownLabels, env);

        if (Object.keys(newMappings).length > 0) {
          const updatedCache = { ...llmCache, ...newMappings };
          await env.LABEL_CACHE.put('label_map', JSON.stringify(updatedCache), { expirationTtl: 604800 });

          for (const item of allItems) {
            if (item.theme === 'series' && item.categoryLabel) {
              const key = item.categoryLabel.toLowerCase().trim();
              if (newMappings[key]) item.theme = newMappings[key];
            }
          }
        }
      }

      // 5. Construire les buckets et répondre
      const buckets = buildBuckets(allItems);

      return new Response(JSON.stringify({
        buckets,
        heroBanners,
        meta: {
          rtbf: rtbfItems.length,
          tf1: tf1Items.length,
          unknownLabelsClassifiedByAI: Object.keys(newMappings ?? {}),
          totalUnknownLabels: unknownLabels.length,
        }
      }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=300' },
      });

    } catch (err: any) {
      console.error('[worker] Error:', err);
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }
  },
};
