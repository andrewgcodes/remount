import { afterEach } from 'vitest';
import { cleanup } from '@testing-library/preact';

afterEach(() => cleanup());

class TestResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}

Object.defineProperty(globalThis, 'ResizeObserver', { value: TestResizeObserver, configurable: true });
Object.defineProperty(window, 'matchMedia', { value: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }), configurable: true });
Object.defineProperty(HTMLCanvasElement.prototype, 'getContext', { value: () => null, configurable: true });
