// A deterministic stand-in for Wikimedia's recentchange stream, used by the
// local mock server and the tests (the real stream isn't reachable from CI).
//
// Every second of "Kafka time" is generated from its own seed, so replaying
// from any `since=` timestamp gives exactly the same events. The mix of wikis,
// change types, bots, late `categorize` events and occasional edit wars is
// modelled on the real stream.

import type { RecentChangeEvent } from "../shared/change.ts";

function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type Rng = () => number;
const pick = <T,>(r: Rng, xs: readonly T[]): T => xs[Math.floor(r() * xs.length)];
function weighted<T>(r: Rng, xs: readonly (readonly [T, number])[]): T {
  const total = xs.reduce((s, [, w]) => s + w, 0);
  let x = r() * total;
  for (const [v, w] of xs) if ((x -= w) <= 0) return v;
  return xs[xs.length - 1][0];
}
function poisson(r: Rng, lambda: number) {
  const L = Math.exp(-lambda);
  let k = 0;
  let p = 1;
  do {
    k++;
    p *= r();
  } while (p > L);
  return k - 1;
}
const uuid = (r: Rng) => {
  const h = () => Math.floor(r() * 16).toString(16);
  const s = (n: number) => Array.from({ length: n }, h).join("");
  return `${s(8)}-${s(4)}-4${s(3)}-${"89ab"[Math.floor(r() * 4)]}${s(3)}-${s(12)}`;
};

const WIKIS = [
  ["wikidatawiki", 36, "www.wikidata.org"],
  ["commonswiki", 21, "commons.wikimedia.org"],
  ["enwiki", 12, "en.wikipedia.org"],
  ["dewiki", 3, "de.wikipedia.org"],
  ["frwiki", 3, "fr.wikipedia.org"],
  ["enwiktionary", 2, "en.wiktionary.org"],
  ["eswiki", 2, "es.wikipedia.org"],
  ["ruwiki", 2, "ru.wikipedia.org"],
  ["jawiki", 1.5, "ja.wikipedia.org"],
  ["itwiki", 1.5, "it.wikipedia.org"],
  ["zhwiki", 1.2, "zh.wikipedia.org"],
  ["ptwiki", 1, "pt.wikipedia.org"],
  ["plwiki", 0.9, "pl.wikipedia.org"],
  ["nlwiki", 0.8, "nl.wikipedia.org"],
  ["arwiki", 0.8, "ar.wikipedia.org"],
  ["ukwiki", 0.8, "uk.wikipedia.org"],
  ["fawiki", 0.6, "fa.wikipedia.org"],
  ["svwiki", 0.6, "sv.wikipedia.org"],
  ["idwiki", 0.5, "id.wikipedia.org"],
  ["trwiki", 0.5, "tr.wikipedia.org"],
  ["kowiki", 0.5, "ko.wikipedia.org"],
  ["viwiki", 0.4, "vi.wikipedia.org"],
  ["hiwiki", 0.35, "hi.wikipedia.org"],
  ["bnwiki", 0.3, "bn.wikipedia.org"],
  ["tawiki", 0.25, "ta.wikipedia.org"],
  ["mrwiki", 0.12, "mr.wikipedia.org"],
  ["metawiki", 0.4, "meta.wikimedia.org"],
] as const;

const BOT_SHARE: Record<string, number> = { wikidatawiki: 0.55, commonswiki: 0.4, enwiktionary: 0.3, enwiki: 0.12 };

const EN_TITLES = [
  "Mumbai", "Python (programming language)", "Apache Kafka", "Apache Flink", "Indian Premier League", "Chandrayaan programme",
  "Monsoon of South Asia", "Diwali", "Large language model", "Climate change", "Taj Mahal", "Lata Mangeshkar",
  "Bollywood", "Premier League", "Artificial intelligence", "Wikipedia", "Linux kernel", "Mount Everest", "Ganges",
  "Solar eclipse", "Stream processing", "Event-driven architecture", "List of tallest buildings", "Cricket World Cup",
  "Formula One", "Machine learning", "Photosynthesis", "Black hole", "Roman Empire", "Kerala", "Bengaluru", "Pune",
  "Delhi Metro", "Indian Railways", "UPI (payment system)", "Rust (programming language)", "Kubernetes", "Zero trust security model",
];
const DE_TITLES = ["Berlin", "Fußball-Bundesliga", "Deutsche Bahn", "Johann Wolfgang von Goethe", "Hamburg", "Rhein", "Oktoberfest"];
const FR_TITLES = ["Paris", "Tour de France", "Victor Hugo", "Lyon", "Marseille", "Louvre", "Révolution française"];
const ES_TITLES = ["Madrid", "Real Madrid Club de Fútbol", "Gabriel García Márquez", "Barcelona", "México"];
const HI_TITLES = ["भारत", "मुंबई", "हिन्दी", "गंगा नदी", "दिल्ली", "क्रिकेट"];
const MR_TITLES = ["पुणे", "मुंबई", "महाराष्ट्र", "गणेशोत्सव"];
const GENERIC = ["Category:Living people", "Template:Infobox settlement", "Portal:Current events", "History", "Geography", "Economy"];
const NS_PREFIX: Record<string, number> = { "Category:": 14, "Template:": 10, "Portal:": 100 };

const USERS = [
  "Aanya.s", "RiverStone", "Quill_and_Ink", "Monsoonwatcher", "DataLibrarian", "Kaveri22", "Edit-a-thon_Pune", "Orbital9", "Hemlock",
  "Saffron.dev", "TeaAndTypos", "Northbound", "ArchiveHound", "LowTide", "Paperclip-7", "Vihaan.k", "CopyeditCat", "Ananya_R",
  "MapMaker88", "Ferrous", "Lumen", "Tukaram.b", "Zephyrine", "Cartographer", "Ishaan.m",
];
const BOTS = ["CommonsDelinker", "Cewbot", "InternetArchiveBot", "KrBot", "SuccuBot", "Pi bot", "DeltaBot", "MatSuBot", "ListeriaBot", "JarBot", "Citation bot"];

const EN_COMMENTS = [
  "Copyedit (minor)", "Added citation", "/* History */ expanded section", "Fixed typo", "Updated infobox", "Rescuing 1 sources and tagging 0 as dead.) #IABot",
  "/* External links */", "Removing unsourced claim", "Updated statistics", "Added short description", "clean up, typo(s) fixed", "Alter: title. Add: doi.",
];
const WD_COMMENTS = [
  "/* wbsetclaim-update:2||1 */ [[Property:P31]]: [[Q5]]", "/* wbsetdescription-add:1|en */ Indian politician", "/* wbsetlabel-add:1|hi */",
  "/* wbcreateclaim-create:1| */ [[Property:P569]]: 12 March 1984", "/* wbsetreference-add:2| */ [[Property:P31]]", "/* wbeditentity-update-languages-short:0||mr */",
];
const COMMONS_COMMENTS = ["[[:Category:Mumbai]] added to category", "Uploaded a work by the uploader", "/* Summary */", "Removing from [[Category:Media needing categories]]"];

const REVERT_COMMENTS = [
  (u: string, r: number) => `Undid revision ${r} by [[Special:Contributions/${u}|${u}]] ([[User talk:${u}|talk]])`,
  (u: string) => `Reverted edits by [[Special:Contributions/${u}|${u}]] ([[User talk:${u}|talk]]) to last version by ClueBot NG`,
  (u: string, r: number) => `Restored revision ${r - 7} by ${u}`,
];

export interface Generated {
  /** Kafka (ingestion) timestamp, ms. */
  kafkaTs: number;
  offset: number;
  event: RecentChangeEvent;
}

const BASE_SEED = 20261009;

/** Pages that get a burst of activity, rotating every 20 minutes. */
function trendingAt(sec: number): string[] {
  const r = mulberry32(BASE_SEED + Math.floor(sec / 1200) * 7919);
  const pool = [...EN_TITLES];
  const out: string[] = [];
  for (let i = 0; i < 4; i++) out.push(pool.splice(Math.floor(r() * pool.length), 1)[0]);
  return out;
}

/** An edit war: one page, three users reverting each other for a few minutes. */
function warAt(sec: number): { title: string; users: string[]; from: number; to: number } | null {
  const slot = Math.floor(sec / 600);
  const r = mulberry32(BASE_SEED ^ (slot * 104729));
  if (r() > 0.4) return null;
  const from = slot * 600 + Math.floor(r() * 300);
  return { title: pick(r, EN_TITLES), users: [pick(r, USERS), pick(r, USERS), pick(r, USERS)], from, to: from + 240 };
}

/** All events whose Kafka timestamp falls in the given second. */
export function eventsForSecond(sec: number): Generated[] {
  const r = mulberry32(BASE_SEED + sec * 2654435761);
  // ~26 events/s with a slow daily-ish wave and noise.
  const rate = 26 * (1 + 0.25 * Math.sin(sec / 1800)) * (0.85 + 0.3 * r());
  const n = poisson(r, rate);
  const trending = trendingAt(sec);
  const war = warAt(sec);
  const out: Generated[] = [];
  for (let i = 0; i < n; i++) {
    const kafkaTs = sec * 1000 + Math.floor((i / Math.max(1, n)) * 1000 + r() * (1000 / Math.max(1, n)));
    const [wiki, , server] = weighted(r, WIKIS.map((w) => [w, w[1]] as const));
    let type = weighted(r, [["edit", 62], ["categorize", 22], ["log", 10], ["new", 6]] as const);
    if (wiki === "wikidatawiki" && type === "categorize") type = "edit";
    const bot = r() < (BOT_SHARE[wiki] ?? 0.12);
    let user = bot ? pick(r, BOTS) : r() < 0.15 ? `2409:40c0:${Math.floor(r() * 65535).toString(16)}::${Math.floor(r() * 255)}` : pick(r, USERS);
    let ns = 0;
    let title: string;
    let comment: string;
    if (wiki === "wikidatawiki") {
      title = `Q${Math.floor(r() * 130_000_000)}`;
      comment = pick(r, WD_COMMENTS);
      if (r() < 0.06) { ns = 120; title = `Property:P${Math.floor(r() * 12000)}`; }
    } else if (wiki === "commonswiki") {
      ns = r() < 0.6 ? 6 : 14;
      title = ns === 6 ? `File:IMG ${Math.floor(r() * 99999)} ${pick(r, ["Mumbai", "Kyoto", "Lisbon", "Nairobi", "Pune", "Oslo"])}.jpg` : `Category:${pick(r, ["Mumbai", "Trains of India", "Monsoon", "Lighthouses", "Rivers of Kerala"])}`;
      comment = pick(r, COMMONS_COMMENTS);
    } else {
      const pool = wiki === "enwiki" ? EN_TITLES : wiki === "dewiki" ? DE_TITLES : wiki === "frwiki" ? FR_TITLES : wiki === "eswiki" ? ES_TITLES : wiki === "hiwiki" ? HI_TITLES : wiki === "mrwiki" ? MR_TITLES : GENERIC;
      title = wiki === "enwiki" && !bot && r() < 0.35 ? pick(r, trending) : pick(r, pool);
      const prefix = Object.keys(NS_PREFIX).find((p) => title.startsWith(p));
      if (prefix) ns = NS_PREFIX[prefix];
      else if (r() < 0.12) { ns = 1; title = `Talk:${title}`; }
      else if (r() < 0.05) { ns = 2; title = `User:${user}`; }
      comment = pick(r, EN_COMMENTS);
    }
    let evSec = sec;
    // Categorize events come from the job queue: their event time is the
    // original edit, often 30 s to several minutes earlier (late data).
    if (type === "categorize") {
      evSec = sec - (r() < 0.7 ? 5 + Math.floor(r() * 40) : 60 + Math.floor(r() * 400));
      ns = 14;
      title = `Category:${pick(r, ["Living people", "Indian cricketers", "Rivers of Maharashtra", "Software", "Stub articles"])}`;
      comment = `[[:${pick(r, EN_TITLES)}]] added to category`;
    } else if (r() < 0.04) {
      evSec = sec - (1 + Math.floor(r() * 3)); // a little out of order
    }
    let logType: string | null = null;
    if (type === "log") {
      logType = weighted(r, [["upload", 30], ["delete", 15], ["block", 6], ["move", 6], ["newusers", 20], ["thanks", 10], ["patrol", 13]] as const);
      ns = logType === "upload" ? 6 : -1;
      comment = logType === "delete" ? "[[WP:CSD#G11|G11]]: Unambiguous advertising" : logType === "block" ? "Vandalism" : "";
    }
    if (type === "new") comment = pick(r, ["Created page with '{{Short description|...'", "New article", "Created by translating the page"]);
    // Edit war: reverts on one page by a few users.
    if (war && sec >= war.from && sec <= war.to && wiki === "enwiki" && r() < 0.035) {
      type = "edit";
      title = war.title;
      ns = 0;
      user = pick(r, war.users);
      comment = pick(r, REVERT_COMMENTS)(pick(r, war.users.filter((u) => u !== user)), 1240000000 + Math.floor(r() * 9999999));
      evSec = sec;
    }
    const oldLen = type === "new" ? null : Math.floor(500 + r() * 90000);
    const sizeDelta = Math.round((r() - 0.4) * (r() < 0.05 ? 40000 : 900));
    const newLen = type === "log" ? null : Math.max(0, (oldLen ?? 0) + (type === "new" ? Math.floor(800 + r() * 9000) : sizeDelta));
    const id = 1_900_000_000 + sec * 50 + i;
    const event: RecentChangeEvent = {
      $schema: "/mediawiki/recentchange/1.0.0",
      meta: {
        uri: `https://${server}/wiki/${encodeURIComponent(title.replace(/ /g, "_"))}`,
        request_id: uuid(r),
        id: uuid(r),
        dt: new Date(evSec * 1000).toISOString().replace(".000Z", "Z"),
        domain: server,
        stream: "mediawiki.recentchange",
        topic: "eqiad.mediawiki.recentchange",
        partition: 0,
        offset: 5_400_000_000 + sec * 60 + i,
      },
      id: type === "log" ? null : id,
      type,
      namespace: ns,
      title,
      comment,
      timestamp: evSec,
      user,
      bot,
      server_url: `https://${server}`,
      server_name: server,
      wiki,
    };
    if (type === "edit" || type === "new") {
      event.minor = r() < 0.2;
      event.patrolled = r() < 0.5;
      event.length = { old: oldLen, new: newLen };
      event.revision = { old: type === "new" ? null : id * 3 - 1, new: id * 3 };
    }
    if (type === "log") {
      event.log_type = logType;
      event.log_action = logType === "upload" ? "upload" : logType === "delete" ? "delete" : logType!;
    }
    out.push({ kafkaTs, offset: event.meta.offset!, event });
  }
  return out;
}

/** Events with Kafka timestamps in [fromMs, toMs). */
export function* eventsBetween(fromMs: number, toMs: number): Generator<Generated> {
  for (let sec = Math.floor(fromMs / 1000); sec * 1000 < toMs; sec++) {
    for (const g of eventsForSecond(sec)) if (g.kafkaTs >= fromMs && g.kafkaTs < toMs) yield g;
  }
}
