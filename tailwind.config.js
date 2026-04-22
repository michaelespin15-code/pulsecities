/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ['./frontend/**/*.html'],
  plugins: [require('daisyui')],
  daisyui: {
    themes: ['dark'],
    logs: false,
  },
  theme: {
    extend: {
      fontFamily: {
        sans: ['Space Grotesk', 'sans-serif'],
      },
    },
  },
  safelist: [
    // Applied dynamically via JS — scanner won't find these via static analysis
    'text-amber-400', 'text-orange-400', 'text-red-400',
    'text-sky-400', 'text-green-400',
    'text-slate-100', 'text-slate-300', 'text-slate-400',
    'text-base-content/70', 'border-base-content/10',
    'flex', 'hidden',
  ],
}
