/** @type {import('tailwindcss').Config} */

// ── Rebuild instructions ──────────────────────────────────────────────────────
// Run from the project root after adding new Tailwind utility classes to templates.
// Requires tools/tailwindcss (downloaded automatically by install.sh).
//
//   ./tools/tailwindcss -i static/css/input.css -o static/css/tw-utilities.css --minify
//   cat tools/daisyui.css static/css/theme-dpc.css static/css/tw-utilities.css \
//       > static/css/tailwind.css && rm static/css/tw-utilities.css
//
// See README.md → "Rebuilding the CSS" for the full workflow.
// ─────────────────────────────────────────────────────────────────────────────

module.exports = {
  content: [
    './templates/**/*.html',
    './static/js/*.js',
  ],
  theme: {
    extend: {
      colors: {
        'dpc-orange':     '#F57F20',
        'dpc-orange-dim': '#C0611A',
        'dpc-navy':       '#194980',
        'dpc-red':        '#F04E23',
      },
      fontFamily: {
        sans: ['Poppins', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      borderRadius: {
        'app':    '6px',
        'app-lg': '10px',
      },
    },
  },
  // Ensure dynamically constructed classes in Jinja templates are not purged
  safelist: [
    // Grid column spans — used via runtime Jinja values in show.html macros
    { pattern: /^col-span-(1|2|3|4|5|6)$/ },
    // DaisyUI alert variants — used in flash message rendering
    'alert-success', 'alert-error', 'alert-warning', 'alert-info',
    // DaisyUI badge variants — used in role/status badges
    'badge-primary', 'badge-secondary', 'badge-accent',
    'badge-success', 'badge-error', 'badge-warning', 'badge-info',
    'badge-ghost', 'badge-outline',
  ],
  plugins: [],
}
