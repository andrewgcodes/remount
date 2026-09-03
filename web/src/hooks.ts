import { useCallback, useEffect, useState } from 'preact/hooks';

export interface AsyncState<T> { data?: T; error?: Error; loading: boolean }

export function usePolling<T>(load: (signal: AbortSignal) => Promise<T>, interval: number): AsyncState<T> & { reload: () => void } {
  const [state, setState] = useState<AsyncState<T>>({ loading: true });
  const [nonce, setNonce] = useState(0);
  const reload = useCallback(() => setNonce((n) => n + 1), []);
  useEffect(() => {
    const controller = new AbortController();
    let timer = 0;
    const run = async () => {
      try {
        const data = await load(controller.signal);
        if (!controller.signal.aborted) setState({ data, loading: false });
      } catch (error) {
        if (!controller.signal.aborted) setState((old) => ({ ...old, error: error as Error, loading: false }));
      }
      if (!controller.signal.aborted) timer = window.setTimeout(run, interval);
    };
    void run();
    return () => { controller.abort(); window.clearTimeout(timer); };
  }, [load, interval, nonce]);
  return { ...state, reload };
}
