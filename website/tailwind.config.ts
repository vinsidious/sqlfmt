import type { Config } from 'tailwindcss';

const config: Config = {
  content: ['./app/**/*.{js,ts,jsx,tsx}', './lib/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        mono: ['"JetBrains Mono"', '"Fira Code"', 'monospace'],
        sans: ['"Inter"', 'system-ui', 'sans-serif'],
      },
      colors: {
        brand: {
          DEFAULT: '#3ECF8E',
          dark: '#24B47E',
          light: '#6EE7B7',
        },
        surface: {
          DEFAULT: '#0A0A0A',
          light: '#111111',
          lighter: '#1a1a1a',
        },
      },
    },
  },
  plugins: [],
};

export default config;
