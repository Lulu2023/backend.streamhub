/**
 * Cloudflare Worker — Aggregateur multi-plateforme StreamHub
 *
 * Plateformes supportées : RTBF Auvio, TF1+, RTL play, FranceTV
 *
 * GET /home                    → { buckets, heroBanners, meta }
 * GET /list?theme=<key>&page=N → { items[], meta }
 * GET /genres?theme=<key>      → { genres: {id,genres[]}[] }
 *
 * Architecture :
 *  - DATA_CACHE KV stocke home_data + list_<theme> précalculés
 *  - Requêtes servies depuis KV < 50ms
 *  - Cron toutes les 3h : refresh en background
 *  - Cache miss (premier démarrage) → fetch live + mise en cache
 *
 * Bindings requis (wrangler.toml) :
 *   [ai]              binding = "AI"
 *   [[kv_namespaces]] binding = "LABEL_CACHE"
 *   [[kv_namespaces]] binding = "DATA_CACHE"
 *   [triggers]        crons = ["0 every-3h * * *"]
 */

export interface Env {
  AI: Ai;
  LABEL_CACHE: KVNamespace;
  DATA_CACHE: KVNamespace;
}

const CACHE_TTL_FRESH = 4 * 60 * 60;
const CACHE_TTL_STALE = 24 * 60 * 60;

// ─── Types ────────────────────────────────────────────────────────────────────

type Platform = 'RTBF' | 'TF1+' | 'RTLplay' | 'FranceTV';

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
  platform: Platform;
  channelLabel?: string;
  resourceType?: string;
  path?: string;
  rating?: string | null;
  theme: ThemeKey;
  genres: string[];
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
  // FranceTV specific
  'series & fictions': 'series', 'series policières': 'thriller', 'series thriller': 'thriller',
  'arts & spectacles': 'culture',
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

const THEMES_WITH_LIST = new Set<ThemeKey>([
  'films', 'series', 'documentaire', 'culture', 'info', 'sport',
  'kids', 'sooner', 'telerealite', 'thriller', 'episodes',
]);

// ─── Config /list ─────────────────────────────────────────────────────────────

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
  episodes:     { type: 'category', path: 'series-35' },
  thriller:     { type: 'category', path: 'series-35' },
  telerealite:  { type: 'category', path: 'series-35' },
};

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

/** RTL play storefronts par thème */
const RTLPLAY_LIST_CONFIG: Partial<Record<ThemeKey, { storefronts: string[] }>> = {
  films:        { storefronts: ['films'] },
  series:       { storefronts: ['series'] },
  telerealite:  { storefronts: ['divertissement'] },
  culture:      { storefronts: ['divertissement'] },
  sport:        { storefronts: ['sport'] },
};

/** FranceTV category slugs par thème */
const FRANCETV_LIST_CONFIG: Partial<Record<ThemeKey, { slugs: string[] }>> = {
  films:        { slugs: ['films'] },
  series:       { slugs: ['series-et-fictions'] },
  documentaire: { slugs: ['documentaires', 'societe'] },
  info:         { slugs: ['info'] },
  culture:      { slugs: ['spectacles-et-culture', 'jeux-et-divertissements'] },
  sport:        { slugs: ['sport'] },
  kids:         { slugs: ['enfants'] },
  thriller:     { slugs: ['series-et-fictions'] },
};

// ─── Vocabulaire de genres unifiés ───────────────────────────────────────────

const GENRE_MAP: Record<string, string> = {
  'film': 'Film', 'films': 'Film', 'cinema': 'Film',
  'telefilm': 'Téléfilm', 'telefilms': 'Téléfilm',
  'biopic': 'Biopic', 'western': 'Western', 'animation': 'Animation',
  'comedie': 'Comédie', 'comedie dramatique': 'Comédie dramatique',
  'comedie romantique': 'Comédie romantique', 'drame': 'Drame', 'romance': 'Romance',
  'action': 'Action', 'aventure': 'Aventure', 'science-fiction': 'Science-fiction',
  'sf': 'Science-fiction', 'fantastique': 'Fantastique', 'horreur': 'Horreur',
  'policier': 'Policier', 'thriller': 'Thriller', 'polar': 'Policier',
  'suspense': 'Thriller', 'crime': 'Crime', 'espionnage': 'Espionnage',
  'serie policiere': 'Policier', 'affaires criminelles': 'Crime',
  'documentaire': 'Documentaire', 'reportage': 'Reportage', 'reportages': 'Reportage',
  'investigation': 'Investigation', 'nature': 'Nature', 'science': 'Science',
  'histoire': 'Histoire', 'societe': 'Société', 'voyage': 'Voyage',
  'enquete': 'Investigation', 'environnement': 'Nature', 'decouvertes': 'Découvertes',
  'humour': 'Humour', 'musique': 'Musique', 'chanson': 'Musique', 'concert': 'Concert',
  'spectacle': 'Spectacle', 'varietes': 'Variétés', 'variete': 'Variétés',
  'talk show': 'Talk-show', 'talk-show': 'Talk-show', 'talkshow': 'Talk-show',
  'late show': 'Talk-show', 'game show': 'Jeux', 'quiz': 'Jeux', 'jeux': 'Jeux',
  'magazine': 'Magazine', 'danse': 'Danse', 'people': 'People', 'lifestyle': 'Lifestyle',
  'divertissement': 'Divertissement', 'emission': 'Émission', 'spectacles': 'Spectacle',
  'actualite': 'Actualité', 'actualites': 'Actualité', 'info': 'Info',
  'journal': 'Journal', 'journal televise': 'Journal', 'politique': 'Politique',
  'economie': 'Économie', 'debat': 'Débat', 'faits divers': 'Faits divers',
  'telerealite': 'Téléréalité', 'docu-realite': 'Docu-réalité',
  'survie': 'Survie', 'mariage': 'Mariage', 'famille': 'Famille',
  'sport': 'Sport', 'football': 'Football', 'rugby': 'Rugby', 'cyclisme': 'Cyclisme',
  'tennis': 'Tennis', 'natation': 'Natation', 'athletisme': 'Athlétisme',
  'formule 1': 'Formule 1', 'f1': 'Formule 1', 'moto': 'Moto',
  'moteurs': 'Sports mécaniques', 'basket': 'Basket', 'boxe': 'Boxe', 'golf': 'Golf',
  'jeunesse': 'Jeunesse', 'kids': 'Jeunesse', 'enfants': 'Jeunesse',
  'anime': 'Animé', 'dessin anime': 'Animé', 'serie animee': 'Animé',
  'animation jeunesse': 'Animé',
  'serie': 'Série', 'sitcom': 'Sitcom', 'feuilleton': 'Feuilleton', 'mini serie': 'Mini-série',
};

function buildGenres(
  categoryLabel: string | undefined,
  typology: string | undefined,
  topics: string[] | undefined,
): string[] {
  const genres = new Set<string>();

  const tryAdd = (raw: string) => {
    if (!raw?.trim()) return;
    const k = normalizeLabel(raw.trim());
    const g = GENRE_MAP[k];
    if (g) { genres.add(g); return; }
    for (const [frag, genre] of Object.entries(GENRE_MAP)) {
      if (frag.length >= 5 && k.includes(frag)) { genres.add(genre); return; }
    }
  };

  const splitAndAdd = (raw: string) => {
    if (!raw) return;
    for (const part of raw.split(/[,\/]|\s+&\s+|\s+et\s+/i)) tryAdd(part);
  };

  if (categoryLabel) splitAndAdd(categoryLabel);
  if (typology)      tryAdd(typology);
  for (const t of (topics ?? [])) tryAdd(t);

  return [...genres];
}

// ─── AI genres classification ─────────────────────────────────────────────────

function extractFirstJSON(text: string): Record<string, unknown> | null {
  const clean = text.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '');
  const start = clean.indexOf('{');
  if (start === -1) return null;

  let depth = 0, inString = false, escape = false;
  for (let i = start; i < clean.length; i++) {
    const ch = clean[i];
    if (escape)          { escape = false; continue; }
    if (ch === '\\')     { escape = true;  continue; }
    if (ch === '"')      { inString = !inString; continue; }
    if (inString)        continue;
    if (ch === '{')      depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) {
        try { return JSON.parse(clean.slice(start, i + 1)); } catch { return null; }
      }
    }
  }
  return null;
}

async function classifyGenresWithAI(
  items: Array<{ id: string; title: string; subtitle?: string; description?: string }>,
  env: Env,
): Promise<Map<string, string[]>> {
  const result = new Map<string, string[]>();
  if (!items.length) return result;

  let genresCache: Record<string, string[]> = {};
  try {
    const raw = await env.LABEL_CACHE.get('genres_map');
    if (raw) genresCache = JSON.parse(raw);
  } catch { /* ignore */ }

  const toClassify: typeof items = [];
  for (const item of items) {
    if (genresCache[item.id]) {
      result.set(item.id, genresCache[item.id]);
    } else {
      toClassify.push(item);
    }
  }
  if (!toClassify.length) return result;

  const BATCH = 15;
  const genreVocab = [...new Set(Object.values(GENRE_MAP))].join(', ');

  for (let i = 0; i < toClassify.length; i += BATCH) {
    const batch = toClassify.slice(i, i + BATCH);
    const itemLines = batch.map((item, idx) => {
      const hint = item.subtitle || (item.description?.slice(0, 80) ?? '');
      return `${idx}: "${item.title}"${hint ? ` — ${hint}` : ''}`;
    }).join('\n');

    const prompt = `Tu es un classificateur de genres pour une plateforme de streaming vidéo francophone.
Pour chaque item ci-dessous (identifié par son index), retourne 1 à 3 genres parmi ce vocabulaire EXACT :
${genreVocab}

Règles strictes :
- Utilise UNIQUEMENT les genres du vocabulaire ci-dessus, mot pour mot, respecte la casse
- 1 genre minimum, 3 maximum par item
- Réponds UNIQUEMENT avec un objet JSON valide, sans texte avant ni après, sans backticks
- Format attendu : { "0": ["Genre1"], "1": ["Genre1", "Genre2"], ... }

Items :
${itemLines}`;

    try {
      const res = await env.AI.run('@cf/meta/llama-3-8b-instruct', {
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 768,
      });

      const text = (res as any).response ?? '';
      const parsed = extractFirstJSON(text) as Record<string, string[]> | null;
      if (!parsed) { console.error('[AI genres] non parseable:', text.slice(0, 300)); continue; }

      let classified = 0;
      for (const [idxStr, rawGenres] of Object.entries(parsed)) {
        const idx = parseInt(idxStr, 10);
        if (isNaN(idx) || idx < 0 || idx >= batch.length) continue;
        const validGenres = (Array.isArray(rawGenres) ? rawGenres : [])
          .filter(g => Object.values(GENRE_MAP).includes(g))
          .slice(0, 3);
        if (!validGenres.length) continue;
        const item = batch[idx];
        result.set(item.id, validGenres);
        genresCache[item.id] = validGenres;
        classified++;
      }
      console.log(`[AI genres] batch ${Math.floor(i/BATCH)+1}: ${batch.length} items → ${classified} classifiés`);
    } catch (err) {
      console.error('[AI genres] batch error:', err);
    }
  }

  try {
    await env.LABEL_CACHE.put('genres_map', JSON.stringify(genresCache), { expirationTtl: 604800 });
  } catch { /* ignore */ }

  return result;
}

// ─── Classification helpers ────────────────────────────────────────────────────

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

  if (typology) {
    const k = normalizeLabel(typology);
    const m = CATEGORY_MAP[k];
    if (m) {
      if (['drame', 'comedie', 'comedie dramatique'].includes(k) && durationSec !== undefined)
        return durationSec > 4800 ? 'films' : 'series';
      return m;
    }
  }

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

  if (topics?.length) {
    for (const t of topics) {
      const k = normalizeLabel(t);
      if (TF1_TOPICS_MAP[k]) return TF1_TOPICS_MAP[k];
      if (CATEGORY_MAP[k]) return CATEGORY_MAP[k];
    }
  }

  return 'series';
}

/**
 * Infère le thème depuis un titre de rangée éditorial (RTL play ou FranceTV).
 */
function inferThemeFromRowTitle(title: string, defaultTheme: ThemeKey = 'series'): ThemeKey {
  const t = normalizeLabel(title);
  if (t.includes('film') || t.includes('cinema') || t.includes('comedie francaise') || t.includes('culte')) return 'films';
  if (t.includes('thriller') || t.includes('polar') || t.includes('policier') || t.includes('crime')) return 'thriller';
  if (t.includes('serie') || t.includes('drama') || t.includes('immanquable')) return 'series';
  if (t.includes('sport') || t.includes('competition') || t.includes('foot') || t.includes('rugby')) return 'sport';
  if (t.includes('info') || t.includes('journal') || t.includes('actualit')) return 'info';
  if (t.includes('concert') || t.includes('musique') || t.includes('culture') || t.includes('spectacle')) return 'culture';
  if (t.includes('document') || t.includes('societe') || t.includes('enquete') || t.includes('reportage')) return 'documentaire';
  if (t.includes('kids') || t.includes('enfant') || t.includes('jeunesse')) return 'kids';
  if (t.includes('divertissement') || t.includes('realite') || t.includes('maries') || t.includes('competition')) return 'telerealite';
  return defaultTheme;
}

// ─── Workers AI (theme classification) ───────────────────────────────────────

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
        if (typeof th === 'string' && th in THEMES) resolved[normalizeLabel(lbl)] = th as ThemeKey;
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

// ─── Fetch TF1 ────────────────────────────────────────────────────────────────

const TF1_GRAPHQL_BASE          = 'https://www.tf1.fr/graphql/fr-be/web';
const TF1_CATEGORY_SLIDERS_QID  = '46f87e88577a61abb1d2a36a715a12d4175caa3d';
const TF1_HOME_QID              = 'c34093152db844db6b7ad9b56df12841f7d13182';
const TF1_BANNER_QID            = 'bd8e6aab9996844dad4ea9a53887adad27d86151';

const TF1_HEADERS = {
  'content-type': 'application/json',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Origin': 'https://www.tf1.fr',
  'Referer': 'https://www.tf1.fr/',
};

async function fetchTF1CategorySliders(slugs: string[]): Promise<any[]> {
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
        { method: 'GET', headers: TF1_HEADERS },
      );
      if (!res.ok) { console.error(`[TF1 cat] ${res.status} slug=${slug}`); continue; }

      const json: any = await res.json();
      const cat = json?.data?.categoryBySlug ?? {};

      for (const cover of (cat.covers ?? [])) {
        if (cover.__typename === 'CoverOfExternalLink') continue;
        if (cover.program || cover.video || cover.__typename === 'CoverOfProgram') allItems.push(cover);
      }

      for (const slider of (cat.sliders ?? [])) {
        const sliderTitle: string = slider.title ?? slider.decoration?.label ?? '';
        const sliderItems = (slider.items ?? slider.programs ?? [])
          .map((i: any) => ({ ...i, _sliderTitle: sliderTitle }));
        allItems.push(...sliderItems);
      }
    } catch (err) {
      console.error(`[TF1 cat] error slug=${slug}:`, err);
    }
  }
  return allItems;
}

async function fetchTF1(): Promise<any> {
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
    fetch(`${TF1_GRAPHQL_BASE}?id=${TF1_HOME_QID}&variables=${homeParams}`, { method: 'GET', headers: TF1_HEADERS }),
    fetch(`https://www.tf1.fr/graphql/web?id=${TF1_BANNER_QID}`, { method: 'GET', headers: TF1_HEADERS }),
  ]);

  const homeJson    = homeRes.status === 'fulfilled' && homeRes.value.ok    ? await homeRes.value.json()    : null;
  const bannersJson = bannersRes.status === 'fulfilled' && bannersRes.value.ok ? await bannersRes.value.json() : null;

  return { ...(homeJson ?? {}), _tf1Banners: bannersJson?.data?.homeCoversByRight ?? [] };
}

// ─── Fetch RTL play ───────────────────────────────────────────────────────────

const RTLPLAY_HEADERS = {
  'User-Agent':          'RTL_PLAY/23.251217 (com.tapptic.rtl.tvi; build:26234; Android 30)',
  'Accept':              'application/json',
  'lfvp-device-segment': 'TV>Android',
  'x-app-version':       '23',
};

async function fetchRTLplayStorefront(storefront: string): Promise<any> {
  const url = `https://lfvp-api.dpgmedia.net/RTL_PLAY/storefronts/${storefront}?itemsPerSwimlane=20`;
  try {
    const res = await fetch(url, { headers: RTLPLAY_HEADERS });
    if (!res.ok) { console.error(`[RTLplay] ${res.status} storefront=${storefront}`); return null; }
    return res.json();
  } catch (err) {
    console.error(`[RTLplay] error storefront=${storefront}:`, err);
    return null;
  }
}

async function fetchRTLplayHome(): Promise<any> {
  return fetchRTLplayStorefront('accueil');
}

// ─── Fetch FranceTV ───────────────────────────────────────────────────────────

const FRANCETV_HEADERS = { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' };

async function fetchFranceTVHome(): Promise<any> {
  try {
    const res = await fetch('https://api-mobile.yatta.francetv.fr/generic/homepage?platform=apps_tv', { headers: FRANCETV_HEADERS });
    if (!res.ok) throw new Error(`FranceTV home ${res.status}`);
    return res.json();
  } catch (err) {
    console.error('[FranceTV home]', err);
    return null;
  }
}

async function fetchFranceTVCategory(slug: string): Promise<any> {
  try {
    const res = await fetch(`https://api-mobile.yatta.francetv.fr/apps/categories/${slug}?platform=apps`, { headers: FRANCETV_HEADERS });
    if (!res.ok) { console.error(`[FranceTV cat] ${res.status} slug=${slug}`); return null; }
    return res.json();
  } catch (err) {
    console.error(`[FranceTV cat] error slug=${slug}:`, err);
    return null;
  }
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
    genres: buildGenres(item.categoryLabel || widgetTitle, undefined, undefined),
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

function normalizeTF1Item(item: any, llmCache: Record<string, ThemeKey>): NormalizedItem | null {
  if (!item) return null;

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

  const videoProg = item.video?.program ?? null;
  const portraitSrc =
    item.decoration?.coverSmall?.sourcesWithScales ??
    item.coverSmall?.sourcesWithScales ??
    prog.decoration?.portrait?.sourcesWithScales ??
    item.decoration?.portrait?.sourcesWithScales ??
    videoProg?.decoration?.portrait?.sourcesWithScales ??
    item.thumbnail?.sourcesWithScales ??
    prog.decoration?.coverSmall?.sourcesWithScales;

  const landscapeSrc =
    prog.decoration?.thumbnail?.sourcesWithScales ??
    item.decoration?.thumbnail?.sourcesWithScales ??
    videoProg?.decoration?.thumbnail?.sourcesWithScales ??
    item.image?.sourcesWithScales ??
    prog.decoration?.background?.sourcesWithScales;

  const prebuiltIllus: Record<string,string> | undefined =
    (item.illustration && typeof item.illustration === 'object' && item.illustration.xs)
      ? (item.illustration as Record<string,string>) : undefined;

  let illustration: Record<string,string> | undefined =
    buildIllustration(portraitSrc ?? []) ?? prebuiltIllus ?? buildIllustration(landscapeSrc ?? []);

  if (!illustration) {
    const fallbackUrl = pickBestUrl([...(portraitSrc ?? []), ...(landscapeSrc ?? [])]);
    if (fallbackUrl) illustration = { xs: fallbackUrl, s: fallbackUrl, m: fallbackUrl, l: fallbackUrl, xl: fallbackUrl };
  }

  const isVideo = item.__typename === 'Video';
  const isFilm  = typology === 'Film' || typology === 'Téléfilm';
  const resourceType: 'PROGRAM' | 'MEDIA' = (isVideo || isFilm) ? 'MEDIA' : 'PROGRAM';

  const ctaVideoId = item.callToAction?.items?.find(
    (i: any) => i.type === 'PLAY' || i.__typename === 'WatchButtonAction',
  )?.video?.id ?? null;
  const mediaId = isVideo ? id : (isFilm ? (ctaVideoId ?? id) : undefined);

  let rating: string | null = prog.rating ?? item.rating ?? null;
  if (rating) rating = rating.replace('CSA_', '').replace('ALL', 'Tout public');

  const allBadges = [...(item.badges ?? []), ...(item.editorBadges ?? [])];
  const stamp = allBadges.length > 0
    ? { label: allBadges[0].label ?? allBadges[0].type ?? '', backgroundColor: '#1a56db', textColor: '#fff' }
    : undefined;

  const enrichedRaw = {
    ...item, id, title,
    subtitle:    prog.decoration?.catchPhrase ?? item.decoration?.catchPhrase,
    description: prog.synopsis ?? prog.decoration?.description ?? item.synopsis,
    illustration, duration, typology,
    slug: prog.slug ?? item.slug, programId: prog.id, programSlug: prog.slug,
    resourceType, platform: 'TF1+', streamId: mediaId, assetId: mediaId,
    hasSubtitles: (prog.hasFrenchDeafSubtitles?.total ?? 0) > 0 || (prog.hasFrenchSubtitles?.total ?? 0) > 0,
    hasAudioDescriptions: (prog.hasDescriptionTrack?.total ?? 0) > 0,
    rating, stamp, ...(isFilm ? { isFilm: true } : {}), isPortrait: true,
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
    genres: buildGenres(item._sliderTitle || undefined, typology, topics),
    _raw: enrichedRaw,
  };
}

// ─── Normalisation RTL play ───────────────────────────────────────────────────

/**
 * Remplace le paramètre _fitwidth/N dans une URL RTL play DPG Media
 * pour générer des variantes à différentes tailles.
 */
function rtlResizeUrl(url: string, width: number): string {
  return url.replace(/_fitwidth\/\d+/, `_fitwidth/${width}`)
            .replace(/_fit\/\d+\/\d+/, `_fitwidth/${width}`);
}

function normalizeRTLItem(
  teaser: any,
  llmCache: Record<string, ThemeKey>,
  rowTitle: string,
  rowType: string,
): NormalizedItem | null {
  if (!teaser || !teaser.title) return null;

  const id    = teaser.detailId ?? teaser.id ?? '';
  const title = teaser.title ?? '';
  if (!id || !title) return null;

  // Inférer le theme depuis le titre de la rangée
  const theme = inferThemeFromRowTitle(rowTitle, 'series');

  // Image portrait (imageUrl) et landscape (heroImageUrl)
  const imgUrl  = teaser.imageUrl ?? '';
  const heroUrl = teaser.heroImageUrl ?? '';

  let illustration: Record<string, string> | undefined;
  if (imgUrl) {
    illustration = {
      xs: rtlResizeUrl(imgUrl, 160),
      s:  rtlResizeUrl(imgUrl, 320),
      m:  rtlResizeUrl(imgUrl, 480),
      l:  heroUrl ? rtlResizeUrl(heroUrl, 800)  : rtlResizeUrl(imgUrl, 640),
      xl: heroUrl ? rtlResizeUrl(heroUrl, 1400) : rtlResizeUrl(imgUrl, 960),
    };
  } else if (heroUrl) {
    illustration = {
      xs: rtlResizeUrl(heroUrl, 400),
      s:  rtlResizeUrl(heroUrl, 600),
      m:  rtlResizeUrl(heroUrl, 800),
      l:  rtlResizeUrl(heroUrl, 1000),
      xl: rtlResizeUrl(heroUrl, 1400),
    };
  }

  // Labels → année de production + nombre de saisons
  const labels: string[] = (teaser.labels ?? []).map((l: any) => l.label ?? '').filter(Boolean);
  const seasonLabel = labels.find(l => /saison/i.test(l));
  const subtitle = teaser.editorialByline ?? seasonLabel ?? undefined;

  return {
    id:           `rtl-${id}`,
    title,
    subtitle,
    description:  teaser.description,
    illustration,
    duration:     undefined,
    categoryLabel: rowTitle || undefined,
    platform:     'RTLplay',
    channelLabel: 'RTL play',
    resourceType: rowType === 'SWIMLANE_PORTRAIT' ? 'PROGRAM' : 'MEDIA',
    path:         `/rtlplay/detail/${id}`,
    rating:       null,
    theme,
    genres:       buildGenres(rowTitle, undefined, undefined),
    _raw:         teaser,
  };
}

// ─── Normalisation FranceTV ───────────────────────────────────────────────────

/**
 * Extrait illustration depuis le tableau images[] FranceTV.
 * Priorité portrait : vignette_2x3 > vignette_3x4 > carre > vignette_16x9
 */
function buildFranceTVIllustration(images: any[]): Record<string, string> | undefined {
  if (!images?.length) return undefined;

  const byType: Record<string, Record<string, string>> = {};
  for (const img of images) {
    if (!img.type || !img.urls) continue;
    byType[img.type] = img.urls;
  }

  // Priorité : portrait d'abord, landscape en fallback
  const portrait = byType['vignette_2x3'] ?? byType['vignette_3x4'] ?? byType['carre'];
  const landscape = byType['vignette_16x9'] ?? byType['background_16x9'];

  const src = portrait ?? landscape;
  if (!src) return undefined;

  // Les clés sont "w:400", "w:800" etc.
  const get = (urls: Record<string, string>, ...widths: string[]) =>
    widths.map(w => urls[`w:${w}`]).find(Boolean) ?? Object.values(urls)[0] ?? '';

  if (portrait) {
    return {
      xs: get(portrait, '400'),
      s:  get(portrait, '400', '800'),
      m:  get(portrait, '800', '400'),
      l:  get(portrait, '1024', '800'),
      xl: get(portrait, '2000', '1024'),
    };
  }
  // Landscape
  return {
    xs: get(landscape!, '400', '800'),
    s:  get(landscape!, '800', '400'),
    m:  get(landscape!, '1024', '800'),
    l:  get(landscape!, '2500', '1024'),
    xl: get(landscape!, '2500', '1024'),
  };
}

function normalizeFranceTVItem(
  item: any,
  llmCache: Record<string, ThemeKey>,
  collectionLabel = '',
): NormalizedItem | null {
  if (!item) return null;

  // Un item FranceTV peut être type "program", "integrale", "unitaire", "flux", "event", "collection"
  const id    = String(item.id ?? '');
  const title = item.label ?? item.title ?? item.episode_title ?? '';
  if (!id || !title) return null;

  // Ignorer les live streams et les collections imbriquées
  if (item.type === 'flux' || item.type === 'collection') return null;

  // Catégorie → thème
  const catLabel = item.category?.label ?? collectionLabel ?? '';
  const subCats: string[] = (item.sub_categories ?? []).map((s: any) => s.label ?? '');

  let theme = resolveTheme(catLabel || undefined, undefined, undefined, item.duration, llmCache);

  // Affiner avec les sous-catégories (ex: "Séries policières" → thriller)
  for (const sub of subCats) {
    const k = normalizeLabel(sub);
    if (k.includes('policier') || k.includes('thriller')) { theme = 'thriller'; break; }
  }

  // Si type unitaire + durée longue → film
  if ((item.type === 'unitaire' || item.type === 'integrale') && item.duration > 4800) {
    if (theme === 'series') theme = 'films';
  }

  const illustration = buildFranceTVIllustration(item.images ?? []);

  // Construction du path selon le channel
  const channelPath = item.broadcast_channel ?? item.channel?.channel_path ?? item.channel?.channel_url ?? '';
  const programPath = item.program_path ?? item.program?.program_path ?? '';
  const path = programPath
    ? `/francetv/${programPath}`
    : `/francetv/${channelPath}/${id}`;

  return {
    id:           `ftv-${id}`,
    title,
    subtitle:     item.headline_title ?? undefined,
    description:  item.synopsis ?? item.description ?? item.medium_description ?? undefined,
    illustration,
    duration:     item.duration ?? undefined,
    categoryLabel: catLabel || undefined,
    platform:     'FranceTV',
    channelLabel: item.channel?.label ?? channelPath ?? 'FranceTV',
    resourceType: item.type === 'unitaire' || item.type === 'integrale' ? 'MEDIA' : 'PROGRAM',
    path,
    rating:       item.rating_csa ?? null,
    theme,
    genres:       buildGenres(catLabel, undefined, subCats),
    _raw:         item,
  };
}

// ─── Extraction items RTLplay depuis un storefront ─────────────────────────────

function extractRTLItems(
  storefrontData: any,
  llmCache: Record<string, ThemeKey>,
): NormalizedItem[] {
  if (!storefrontData) return [];
  const items: NormalizedItem[] = [];

  const EXCL_TYPES = new Set(['TOP_BANNER', 'MARKETING']);

  for (const row of (storefrontData.rows ?? [])) {
    if (EXCL_TYPES.has(row.rowType)) continue;
    const rowTitle: string = row.title ?? '';
    const rowType: string  = row.rowType ?? '';
    const teasers: any[]   = row.teasers ?? row.items ?? [];

    for (const teaser of teasers) {
      const item = normalizeRTLItem(teaser, llmCache, rowTitle, rowType);
      if (item) items.push(item);
    }
  }
  return items;
}

// ─── Extraction items FranceTV depuis une réponse API ─────────────────────────

function extractFranceTVItems(
  data: any,
  llmCache: Record<string, ThemeKey>,
): NormalizedItem[] {
  if (!data) return [];
  const items: NormalizedItem[] = [];

  const collections: any[] = data.collections ?? [];
  for (const col of collections) {
    // Ignorer les collections de type lien ou navigation pure
    if (col.type === 'link' || col.type === 'playlist_categories') continue;
    const colLabel: string = col.label ?? '';
    const colItems: any[]  = col.items ?? [];

    for (const raw of colItems) {
      const item = normalizeFranceTVItem(raw, llmCache, colLabel);
      if (item) items.push(item);
    }
  }
  return items;
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

/**
 * Interleave 4 plateformes pour la homepage : RTBF, TF1, RTLplay, FranceTV
 * afin d'avoir une diversité maximale dans chaque bucket.
 */
function buildBuckets(
  rtbfItems: NormalizedItem[],
  tf1Items:  NormalizedItem[],
  rtlItems:  NormalizedItem[],
  ftvItems:  NormalizedItem[],
): ThematicBucket[] {
  const groups: Record<ThemeKey, { rtbf: NormalizedItem[]; tf1: NormalizedItem[]; rtl: NormalizedItem[]; ftv: NormalizedItem[] }> = {} as any;
  for (const th of BUCKET_ORDER) {
    groups[th] = { rtbf: [], tf1: [], rtl: [], ftv: [] };
  }

  const addTo = (map: Record<ThemeKey, NormalizedItem[]>, item: NormalizedItem) => {
    if (map[item.theme]) map[item.theme].push(item);
  };

  for (const i of rtbfItems) addTo(groups[i.theme] ? { [i.theme]: groups[i.theme].rtbf } as any : {}, i);
  for (const i of tf1Items)  addTo(groups[i.theme] ? { [i.theme]: groups[i.theme].tf1 }  as any : {}, i);
  for (const i of rtlItems)  addTo(groups[i.theme] ? { [i.theme]: groups[i.theme].rtl }  as any : {}, i);
  for (const i of ftvItems)  addTo(groups[i.theme] ? { [i.theme]: groups[i.theme].ftv }  as any : {}, i);

  // Réattribuer correctement (boucle ci-dessus est incorrecte, refaire proprement)
  const G: Record<ThemeKey, Record<Platform, NormalizedItem[]>> = {} as any;
  for (const th of BUCKET_ORDER) {
    G[th] = { RTBF: [], 'TF1+': [], RTLplay: [], FranceTV: [] };
  }
  for (const i of rtbfItems) if (G[i.theme]) G[i.theme]['RTBF'].push(i);
  for (const i of tf1Items)  if (G[i.theme]) G[i.theme]['TF1+'].push(i);
  for (const i of rtlItems)  if (G[i.theme]) G[i.theme]['RTLplay'].push(i);
  for (const i of ftvItems)  if (G[i.theme]) G[i.theme]['FranceTV'].push(i);

  return BUCKET_ORDER.map(theme => {
    const r = G[theme]['RTBF'], t = G[theme]['TF1+'],
          l = G[theme]['RTLplay'], f = G[theme]['FranceTV'];
    const merged: NormalizedItem[] = [];
    let ri = 0, ti = 0, li = 0, fi = 0;
    // Interleave round-robin 4 plateformes
    while (ri < r.length || ti < t.length || li < l.length || fi < f.length) {
      if (ri < r.length) merged.push(r[ri++]);
      if (ti < t.length) merged.push(t[ti++]);
      if (li < l.length) merged.push(l[li++]);
      if (fi < f.length) merged.push(f[fi++]);
    }
    return {
      theme, label: THEMES[theme].label, emoji: THEMES[theme].emoji,
      items: merged, hasMore: THEMES_WITH_LIST.has(theme),
    };
  }).filter(b => b.items.length > 0);
}

// ─── Banners RTBF ─────────────────────────────────────────────────────────────

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

    const bgS = [...(deco.cover?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 1) - (a.scale ?? 1));
    const pS  = [...(deco.coverSmall?.sourcesWithScales ?? [])].sort((a: any, b: any) => (b.scale ?? 1) - (a.scale ?? 1));
    const bgB = bgS.find((s: any) => s.scale >= 2)?.url ?? bgS[0]?.url ?? '';
    const bgA = bgS[0]?.url ?? '';
    const pB  = pS.find((s: any) => s.scale >= 2)?.url ?? pS[0]?.url ?? '';
    const pA  = pS[0]?.url ?? '';
    const image = (bgB || pB) ? { xs: pA || bgA, s: pB || bgA, m: bgA || pA, l: bgB || pB, xl: bgB || pB } : null;
    const videoUrl = deco.video?.sources?.[0]?.url ?? null;
    const typename = cover.__typename ?? '';
    let contentType: string, contentId: string, contentSlug: string | null;

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
      contentId: String(contentId ?? id), contentSlug, programId: prog.id ?? null, programSlug: prog.slug ?? null,
      typology: topSrc.typology ?? prog.typology ?? '',
      topics:   topSrc.topics   ?? prog.topics   ?? [],
      season:   vid.season   ?? ctaV?.season   ?? null,
      episode:  vid.episode  ?? ctaV?.episode  ?? null,
      duration: vid.playingInfos?.duration ?? ctaV?.playingInfos?.duration ?? null,
      rights:   vid.rights ?? ctaV?.rights ?? cover.rights ?? [],
      backgroundColor: '#000000', platform: 'TF1+',
    });
  }
  // Fallback slider
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

/** Banners RTL play — depuis le TOP_BANNER row */
function buildRTLBanners(storefrontData: any): any[] {
  if (!storefrontData) return [];
  const banners: any[] = [];

  const topRow = (storefrontData.rows ?? []).find((r: any) => r.rowType === 'TOP_BANNER');
  if (topRow?.teaser) {
    const t = topRow.teaser;
    const id = t.detailId ?? t.id ?? '';
    if (id && t.title) {
      banners.push({
        id: `rtl-banner-${id}`,
        coverId: id,
        title: t.title,
        description: t.description ?? '',
        image: t.heroImageUrl
          ? { xs: t.imageUrl ?? t.heroImageUrl, s: t.imageUrl ?? t.heroImageUrl, m: t.heroImageUrl, l: t.heroImageUrl, xl: t.heroImageUrl }
          : null,
        videoUrl: null,
        deepLink: `/rtlplay/detail/${id}`,
        contentType: 'program',
        contentId: id,
        contentSlug: null,
        backgroundColor: '#e30613',
        platform: 'RTLplay',
      });
    }
  }
  return banners;
}

/** Banners FranceTV — depuis la collection mise_en_avant */
function buildFranceTVBanners(homeData: any): any[] {
  if (!homeData) return [];
  const banners: any[] = [];

  const highlight = (homeData.collections ?? []).find((c: any) => c.type === 'mise_en_avant');
  if (!highlight) return banners;

  for (const item of (highlight.items ?? []).slice(0, 5)) {
    const id    = String(item.id ?? '');
    const title = item.label ?? item.title ?? '';
    if (!id || !title) continue;

    const illustration = buildFranceTVIllustration(item.images ?? []);
    const channelPath  = item.broadcast_channel ?? item.channel?.channel_path ?? '';
    const programPath  = item.program_path ?? '';

    banners.push({
      id:           `ftv-banner-${id}`,
      coverId:      id,
      title,
      description:  item.synopsis ?? item.headline_title ?? '',
      image:        illustration ? { xs: illustration.xs, s: illustration.s, m: illustration.m, l: illustration.l, xl: illustration.xl } : null,
      videoUrl:     null,
      deepLink:     programPath ? `/francetv/${programPath}` : `/francetv/${channelPath}/${id}`,
      contentType:  item.type === 'unitaire' || item.type === 'integrale' ? 'video' : 'program',
      contentId:    id,
      contentSlug:  item.program_path ?? null,
      backgroundColor: '#003189',
      platform:     'FranceTV',
    });
  }
  return banners;
}

// ─── Helpers cache ────────────────────────────────────────────────────────────

const HEAVY_KEYS = new Set(['decoration', 'badges', 'editorBadges', 'callToAction', 'sliders', 'covers', 'program', '_sliderTitle']);

function slimItem(item: NormalizedItem): NormalizedItem {
  if (!item._raw) return item;
  return {
    ...item,
    _raw: Object.fromEntries(Object.entries(item._raw).filter(([k]) => !HEAVY_KEYS.has(k))),
  };
}

/** Clés KV — version v4 pour invalider l'ancien cache */
const listKey = (theme: ThemeKey) => `list_v4_${theme}`;
const homeKey = () => 'home_v4';

// ─── Précalcul d'un thème ─────────────────────────────────────────────────────

async function buildListData(theme: ThemeKey, env: Env): Promise<{
  items: NormalizedItem[];
  meta: { rtbf: number; tf1: number; rtl: number; ftv: number; total: number; builtAt: number };
}> {
  const llmCacheRaw = await env.LABEL_CACHE.get('label_map').catch(() => null);
  const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

  const rtbfCfg   = RTBF_LIST_CONFIG[theme];
  const tf1Cfg    = TF1_LIST_CONFIG[theme];
  const rtlCfg    = RTLPLAY_LIST_CONFIG[theme];
  const ftvCfg    = FRANCETV_LIST_CONFIG[theme];

  // ── RTBF ────────────────────────────────────────────────────────────────────
  const rtbfPromise: Promise<{ items: any[]; widgetTitle: string }[]> = rtbfCfg
    ? rtbfCfg.type === 'category'
      ? fetchRTBFCategoryAll(rtbfCfg.path)
      : Promise.all(
          rtbfCfg.ids.map(id =>
            fetchRTBFWidgetAll(`https://bff-service.rtbf.be/auvio/v1.23/widgets/${id}`, 6)
              .then(items => ({ items, widgetTitle: rtbfCfg.forceTitle ?? '' })),
          ),
        )
    : Promise.resolve([]);

  // ── TF1 ─────────────────────────────────────────────────────────────────────
  const tf1Promise: Promise<any[]> = tf1Cfg
    ? fetchTF1CategorySliders(tf1Cfg.slugs)
    : Promise.resolve([]);

  // ── RTLplay ─────────────────────────────────────────────────────────────────
  const rtlPromise: Promise<NormalizedItem[]> = rtlCfg
    ? Promise.all(rtlCfg.storefronts.map(s => fetchRTLplayStorefront(s)))
        .then(results => results.flatMap(d => extractRTLItems(d, llmCache)))
    : Promise.resolve([]);

  // ── FranceTV ────────────────────────────────────────────────────────────────
  const ftvPromise: Promise<NormalizedItem[]> = ftvCfg
    ? Promise.all(ftvCfg.slugs.map(s => fetchFranceTVCategory(s)))
        .then(results => results.flatMap(d => extractFranceTVItems(d, llmCache)))
    : Promise.resolve([]);

  const [rtbfRes, tf1Res, rtlRes, ftvRes] = await Promise.allSettled([rtbfPromise, tf1Promise, rtlPromise, ftvPromise]);

  let rtbfItems: NormalizedItem[] = [];
  if (rtbfRes.status === 'fulfilled') {
    for (const { items, widgetTitle } of rtbfRes.value) {
      for (const raw of items) {
        const item = normalizeRTBFItem(raw, llmCache, widgetTitle);
        if (item) rtbfItems.push(item);
      }
    }
  } else console.error(`[buildList:${theme}] RTBF error:`, rtbfRes.reason);

  let tf1Items: NormalizedItem[] = [];
  if (tf1Res.status === 'fulfilled') {
    for (const raw of tf1Res.value) {
      const item = normalizeTF1Item(raw, llmCache);
      if (item) tf1Items.push(item);
    }
  } else console.error(`[buildList:${theme}] TF1 error:`, tf1Res.reason);

  let rtlItems: NormalizedItem[] = rtlRes.status === 'fulfilled' ? rtlRes.value : [];
  let ftvItems: NormalizedItem[] = ftvRes.status === 'fulfilled' ? ftvRes.value : [];

  // Filtrage thématique post-normalisation
  if (theme === 'thriller') {
    rtbfItems = rtbfItems.filter(i => i.theme === 'thriller');
    tf1Items  = tf1Items.filter(i => i.theme === 'thriller');
    ftvItems  = ftvItems.filter(i => i.theme === 'thriller');
  } else if (theme === 'episodes') {
    rtbfItems = rtbfItems.filter(i => i.theme === 'episodes');
    tf1Items  = tf1Items.filter(i => i.theme === 'episodes' || i.theme === 'series');
  } else if (theme === 'telerealite') {
    rtbfItems = rtbfItems.filter(i => i.theme === 'telerealite');
    tf1Items  = tf1Items.filter(i => i.theme === 'telerealite');
    rtlItems  = rtlItems.filter(i => i.theme === 'telerealite');
  }

  rtbfItems = deduplicate(rtbfItems);
  tf1Items  = deduplicate(tf1Items);
  rtlItems  = deduplicate(rtlItems);
  ftvItems  = deduplicate(ftvItems);

  // Classification IA des genres sur les items sans genres
  const allItems = [...rtbfItems, ...tf1Items, ...rtlItems, ...ftvItems];
  const needsAI  = allItems.filter(i => i.genres.length === 0);
  if (needsAI.length > 0) {
    console.log(`[buildList:${theme}] AI genres: ${needsAI.length} items`);
    const aiResult = await classifyGenresWithAI(
      needsAI.map(i => ({ id: i.id, title: i.title, subtitle: i.subtitle, description: i.description })),
      env,
    );
    for (const item of allItems) {
      if (item.genres.length === 0) {
        const g = aiResult.get(item.id);
        if (g?.length) item.genres = g;
      }
    }
  }

  // Interleave 4 plateformes
  const merged: NormalizedItem[] = [];
  let ri = 0, ti = 0, li = 0, fi = 0;
  while (ri < rtbfItems.length || ti < tf1Items.length || li < rtlItems.length || fi < ftvItems.length) {
    if (ri < rtbfItems.length) merged.push(rtbfItems[ri++]);
    if (ti < tf1Items.length)  merged.push(tf1Items[ti++]);
    if (li < rtlItems.length)  merged.push(rtlItems[li++]);
    if (fi < ftvItems.length)  merged.push(ftvItems[fi++]);
  }

  console.log(`[buildList:${theme}] rtbf=${rtbfItems.length} tf1=${tf1Items.length} rtl=${rtlItems.length} ftv=${ftvItems.length} total=${merged.length}`);

  return {
    items: merged.map(slimItem),
    meta: {
      rtbf: rtbfItems.length, tf1: tf1Items.length,
      rtl: rtlItems.length, ftv: ftvItems.length,
      total: merged.length, builtAt: Date.now(),
    },
  };
}

// ─── Précalcul home ────────────────────────────────────────────────────────────

async function buildHomeData(env: Env): Promise<{
  buckets: ThematicBucket[];
  heroBanners: any[];
  meta: any;
}> {
  const [rtbfResult, tf1Result, rtlResult, ftvResult] = await Promise.allSettled([
    fetchRTBF(),
    fetchTF1(),
    fetchRTLplayHome(),
    fetchFranceTVHome(),
  ]);

  const rtbfHome = rtbfResult.status === 'fulfilled' ? rtbfResult.value : null;
  const tf1Raw   = tf1Result.status  === 'fulfilled' ? tf1Result.value  : null;
  const rtlRaw   = rtlResult.status  === 'fulfilled' ? rtlResult.value  : null;
  const ftvRaw   = ftvResult.status  === 'fulfilled' ? ftvResult.value  : null;

  if (rtbfResult.status === 'rejected') console.error('[buildHome] RTBF:', rtbfResult.reason);
  if (tf1Result.status  === 'rejected') console.error('[buildHome] TF1:',  tf1Result.reason);
  if (rtlResult.status  === 'rejected') console.error('[buildHome] RTL:',  rtlResult.reason);
  if (ftvResult.status  === 'rejected') console.error('[buildHome] FTV:',  ftvResult.reason);

  // ── Hero banners ─────────────────────────────────────────────────────────────
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
    ...(rtlRaw   ? buildRTLBanners(rtlRaw)                    : []),
    ...(ftvRaw   ? buildFranceTVBanners(ftvRaw)               : []),
  ];

  // ── LLM cache ─────────────────────────────────────────────────────────────────
  const llmCacheRaw = await env.LABEL_CACHE.get('label_map').catch(() => null);
  const llmCache: Record<string, ThemeKey> = llmCacheRaw ? JSON.parse(llmCacheRaw) : {};

  // ── Normaliser items RTBF ─────────────────────────────────────────────────────
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

  // ── Normaliser items TF1 ──────────────────────────────────────────────────────
  let tf1Items: NormalizedItem[] = [];
  if (tf1Raw) {
    for (const slider of tf1Raw.data?.homeSliders ?? []) {
      for (const item of slider.items ?? []) {
        const n = normalizeTF1Item(item, llmCache);
        if (n) tf1Items.push(n);
      }
    }
  }

  // ── Normaliser items RTLplay ──────────────────────────────────────────────────
  let rtlItems: NormalizedItem[] = extractRTLItems(rtlRaw, llmCache);

  // ── Normaliser items FranceTV ─────────────────────────────────────────────────
  let ftvItems: NormalizedItem[] = extractFranceTVItems(ftvRaw, llmCache);

  rtbfItems = deduplicate(rtbfItems);
  tf1Items  = deduplicate(tf1Items);
  rtlItems  = deduplicate(rtlItems);
  ftvItems  = deduplicate(ftvItems);

  // ── AI pour labels inconnus (home uniquement, non bloquant) ───────────────────
  const allForAI = [...rtbfItems, ...tf1Items, ...rtlItems, ...ftvItems];
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
      rtlItems  = allForAI.filter(i => i.platform === 'RTLplay');
      ftvItems  = allForAI.filter(i => i.platform === 'FranceTV');
    }
  }

  const buckets = buildBuckets(rtbfItems, tf1Items, rtlItems, ftvItems);
  console.log(`[buildHome] rtbf=${rtbfItems.length} tf1=${tf1Items.length} rtl=${rtlItems.length} ftv=${ftvItems.length} buckets=${buckets.length}`);

  return {
    buckets,
    heroBanners,
    meta: {
      rtbf: rtbfItems.length, tf1: tf1Items.length,
      rtl: rtlItems.length, ftv: ftvItems.length,
      buckets: buckets.length,
      unknownLabelsClassifiedByAI: Object.keys(newMappings),
      builtAt: Date.now(),
    },
  };
}

// ─── Refresh complet ──────────────────────────────────────────────────────────

async function refreshAll(env: Env): Promise<void> {
  console.log('[refresh] Démarrage refresh complet (4 plateformes)');

  try {
    const homeData = await buildHomeData(env);
    await env.DATA_CACHE.put(homeKey(), JSON.stringify(homeData), { expirationTtl: CACHE_TTL_STALE });
    console.log('[refresh] home OK');
  } catch (err) {
    console.error('[refresh] home ERREUR:', err);
  }

  const themes = [...THEMES_WITH_LIST] as ThemeKey[];
  for (let i = 0; i < themes.length; i += 4) {
    const batch = themes.slice(i, i + 4);
    await Promise.allSettled(batch.map(async theme => {
      try {
        const data = await buildListData(theme, env);
        await env.DATA_CACHE.put(listKey(theme), JSON.stringify(data), { expirationTtl: CACHE_TTL_STALE });
        console.log(`[refresh] list:${theme} OK (${data.meta.total} items)`);
      } catch (err) {
        console.error(`[refresh] list:${theme} ERREUR:`, err);
      }
    }));
  }
  console.log('[refresh] Terminé');
}

// ─── Handler principal ────────────────────────────────────────────────────────

export default {

  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(refreshAll(env));
  },

  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const cors = {
      'Access-Control-Allow-Origin':  '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });

    // ── /genres?theme=X ──────────────────────────────────────────────────────
    if (url.pathname === '/genres') {
      const theme = url.searchParams.get('theme') as ThemeKey | null;
      if (!theme || !(theme in THEMES)) {
        return new Response(JSON.stringify({ error: 'theme invalide' }), {
          status: 400, headers: { ...cors, 'Content-Type': 'application/json' },
        });
      }
      try {
        const cached = await env.DATA_CACHE.get(listKey(theme));
        if (cached) {
          const data = JSON.parse(cached) as { items: NormalizedItem[] };
          const genres = data.items.map(i => ({ id: i.id, genres: i.genres }));
          return new Response(JSON.stringify({ genres }), {
            headers: { ...cors, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=600' },
          });
        }
        return new Response(JSON.stringify({ genres: [] }), {
          headers: { ...cors, 'Content-Type': 'application/json' },
        });
      } catch (err: any) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...cors, 'Content-Type': 'application/json' },
        });
      }
    }

    // ── /list?theme=X&page=N ─────────────────────────────────────────────────
    if (url.pathname === '/list') {
      const theme = url.searchParams.get('theme') as ThemeKey | null;
      if (!theme || !(theme in THEMES)) {
        return new Response(JSON.stringify({ error: 'theme invalide' }), {
          status: 400, headers: { ...cors, 'Content-Type': 'application/json' },
        });
      }

      try {
        let cachedRaw = await env.DATA_CACHE.get(listKey(theme));

        if (!cachedRaw) {
          console.log(`[/list] Cache miss theme=${theme}, build live`);
          const data = await buildListData(theme, env);
          cachedRaw = JSON.stringify(data);
          ctx.waitUntil(
            env.DATA_CACHE.put(listKey(theme), cachedRaw, { expirationTtl: CACHE_TTL_STALE })
          );
        }

        const fullData = JSON.parse(cachedRaw) as {
          items: NormalizedItem[];
          meta: { rtbf: number; tf1: number; rtl: number; ftv: number; total: number; builtAt: number };
        };

        const PAGE_SIZE  = 48;
        const page       = Math.max(1, parseInt(url.searchParams.get('page') ?? '1', 10));
        const start      = (page - 1) * PAGE_SIZE;
        const pageItems  = fullData.items.slice(start, start + PAGE_SIZE);
        const totalPages = Math.ceil(fullData.items.length / PAGE_SIZE);

        return new Response(JSON.stringify({
          theme,
          label: THEMES[theme].label,
          emoji: THEMES[theme].emoji,
          items: pageItems,
          meta: {
            ...fullData.meta,
            page, pageSize: PAGE_SIZE, totalPages,
            hasMore: page < totalPages,
            cachedAt: fullData.meta.builtAt,
          },
        }), {
          headers: { ...cors, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=300' },
        });

      } catch (err: any) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500, headers: { ...cors, 'Content-Type': 'application/json' },
        });
      }
    }

    // ── /home ────────────────────────────────────────────────────────────────
    if (url.pathname !== '/home') {
      return new Response('Not found', { status: 404, headers: cors });
    }

    try {
      let cachedRaw = await env.DATA_CACHE.get(homeKey());

      if (!cachedRaw) {
        console.log('[/home] Cache miss, build live');
        const data = await buildHomeData(env);
        cachedRaw = JSON.stringify(data);
        ctx.waitUntil(
          env.DATA_CACHE.put(homeKey(), cachedRaw, { expirationTtl: CACHE_TTL_STALE })
        );
      }

      return new Response(cachedRaw, {
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
