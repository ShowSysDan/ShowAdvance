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
        // DaisyUI semantic colors — map to CSS variables so Tailwind generates
        // utilities like bg-primary, text-base-content/50, bg-primary/10, etc.
        'primary':          'hsl(var(--p) / <alpha-value>)',
        'primary-focus':    'hsl(var(--pf) / <alpha-value>)',
        'primary-content':  'hsl(var(--pc) / <alpha-value>)',
        'secondary':        'hsl(var(--s) / <alpha-value>)',
        'secondary-focus':  'hsl(var(--sf) / <alpha-value>)',
        'secondary-content':'hsl(var(--sc) / <alpha-value>)',
        'accent':           'hsl(var(--a) / <alpha-value>)',
        'accent-focus':     'hsl(var(--af) / <alpha-value>)',
        'accent-content':   'hsl(var(--ac) / <alpha-value>)',
        'neutral':          'hsl(var(--n) / <alpha-value>)',
        'neutral-focus':    'hsl(var(--nf) / <alpha-value>)',
        'neutral-content':  'hsl(var(--nc) / <alpha-value>)',
        'base-100':         'hsl(var(--b1) / <alpha-value>)',
        'base-200':         'hsl(var(--b2) / <alpha-value>)',
        'base-300':         'hsl(var(--b3) / <alpha-value>)',
        'base-content':     'hsl(var(--bc) / <alpha-value>)',
        'info':             'hsl(var(--in) / <alpha-value>)',
        'info-content':     'hsl(var(--inc) / <alpha-value>)',
        'success':          'hsl(var(--su) / <alpha-value>)',
        'success-content':  'hsl(var(--suc) / <alpha-value>)',
        'warning':          'hsl(var(--wa) / <alpha-value>)',
        'warning-content':  'hsl(var(--wac) / <alpha-value>)',
        'error':            'hsl(var(--er) / <alpha-value>)',
        'error-content':    'hsl(var(--erc) / <alpha-value>)',
        // DPC brand colors
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
