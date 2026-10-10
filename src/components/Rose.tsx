/** The compass rose: Tideline's mark. North is magenta, like a chart's magnetic variation. */
export function Rose({ size = 30 }: { size?: number }) {
  return (
    <svg viewBox="0 0 64 64" width={size} height={size} aria-hidden="true">
      <circle cx="32" cy="32" r="23" fill="none" stroke="var(--ink)" strokeWidth="1.6" />
      <circle cx="32" cy="32" r="27" fill="none" stroke="var(--ink)" strokeWidth="0.8" strokeDasharray="1.5 2.6" />
      <path d="M32 3 L36.5 32 L32 61 L27.5 32 Z" fill="var(--ink)" />
      <path d="M32 3 L36.5 32 L27.5 32 Z" fill="var(--magenta)" />
      <path d="M3 32 L32 27.5 L61 32 L32 36.5 Z" fill="var(--paper)" stroke="var(--ink)" strokeWidth="1.4" />
      <circle cx="32" cy="32" r="2.6" fill="var(--paper)" stroke="var(--ink)" strokeWidth="1.4" />
    </svg>
  );
}
