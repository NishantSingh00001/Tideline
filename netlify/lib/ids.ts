// Short random ids for links (no look-alike characters).
const ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz";
export const randomId = (n: number) => Array.from(crypto.getRandomValues(new Uint8Array(n)), (b) => ALPHABET[b % ALPHABET.length]).join("");
export const ID_RE = (n: number) => new RegExp(`^[2-9a-hjkmnp-z]{${n}}$`);
