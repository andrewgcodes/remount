import { defineConfig } from 'vite';
import preact from '@preact/preset-vite';

export default defineConfig({
  base: '/console/',
  plugins: [preact()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: false,
    target: ['chrome111', 'edge111', 'firefox114', 'safari16.4'],
  },
});
