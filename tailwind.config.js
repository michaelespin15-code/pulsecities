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
        sans: ['DM Sans', 'sans-serif'],
      },
      // House palette. The stock stops are recognizable at a glance, so every
      // color the UI uses is retuned a step off the framework values while
      // keeping the same roles and contrast.
      colors: {
        slate: {
          100: '#eef2f5',
          300: '#c9d2da',
          400: '#93a1ad',
          500: '#677686',
          600: '#46525f',
          700: '#303c4c',
          800: '#1b2534',
          900: '#111823',
        },
        orange: { 400: '#f0854b', 500: '#ed6317' },
        sky: { 400: '#6fb1d8' },
        red: { 400: '#ec6a5e', 500: '#e4483b' },
        amber: { 400: '#d9a441' },
        green: { 400: '#6fa287' },
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
