// The five "projects" the live page groups changes into, in the fixed order
// their colours are assigned (validated with the dataviz palette checker).

export const PROJECTS = ["Wikidata", "Commons", "English Wikipedia", "Other Wikipedias", "Other projects"] as const;
export type Project = (typeof PROJECTS)[number];

/** Same rule as the CASE expression in the pulse query. */
export function projectLane(wiki: string, server: string): number {
  if (wiki === "wikidatawiki") return 0;
  if (wiki === "commonswiki") return 1;
  if (wiki === "enwiki") return 2;
  if (server.endsWith(".wikipedia.org")) return 3;
  return 4;
}

export const PROJECT_COLORS = ["#24679f", "#1a9479", "#b5781a", "#7f61c4", "#9b4f2f"];
export const projectIndex = (name: string) => PROJECTS.indexOf(name as Project);
