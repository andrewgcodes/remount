import { render } from 'preact';
import { Console } from './App';
import { APIClient } from './api';
import { loadConfig } from './config';
import './styles.css';

async function main() {
  const host = document.getElementById('app');
  if (!host) throw new Error('console root is missing');
  try {
    const config = await loadConfig();
    render(<Console api={new APIClient(config)} config={config}/>, host);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'unknown startup error';
    host.textContent = `Remount console could not start: ${message}`;
  }
}

void main();
