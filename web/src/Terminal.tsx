import { useEffect, useRef, useState } from 'preact/hooks';
import { Terminal as XTerminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import '@xterm/xterm/css/xterm.css';
import type { APIClient } from './api';

interface Props { api: APIClient; workspace: string; session: string; replayMinutes: number; initialNext: number }

export function Terminal({ api, workspace, session, replayMinutes, initialNext }: Props) {
  const host = useRef<HTMLDivElement>(null);
  const [connection, setConnection] = useState('connecting');

  useEffect(() => {
    if (!host.current) return;
    const term = new XTerminal({
      allowProposedApi: false,
      convertEol: false,
      cursorBlink: true,
      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
      fontSize: 13,
      screenReaderMode: true,
      scrollback: 100_000,
      theme: { background: '#0b0f17', foreground: '#d9e1ee', cursor: '#65d9b7' },
    });
    const fit = new FitAddon();
    term.loadAddon(fit);
    term.open(host.current);
    fit.fit();
    const since = replayMinutes > 0 ? new Date(Date.now() - replayMinutes * 60_000).toISOString() : undefined;
    let socket: WebSocket | undefined;
    let retryTimer = 0;
    let attempts = 0;
    let cursor: number | undefined;
    let disposed = false;
    let terminalExit = false;
    const decode = (data: string) => {
      const binary = atob(data);
      return Uint8Array.from(binary, (value) => value.charCodeAt(0));
    };
    const connect = () => {
      if (disposed || terminalExit) return;
      setConnection(attempts ? 'reconnecting' : 'connecting');
      const replay = cursor === undefined ? (since ? { since } : { from: initialNext }) : { from: cursor };
      socket = new WebSocket(api.terminalURL(workspace, session, replay), api.terminalProtocols());
      socket.binaryType = 'arraybuffer';
      socket.onopen = () => { attempts = 0; setConnection('live'); };
      socket.onclose = () => {
        if (disposed || terminalExit) { setConnection('closed'); return; }
        attempts += 1;
        setConnection('reconnecting');
        retryTimer = window.setTimeout(connect, Math.min(5000, 250 * 2 ** Math.min(attempts, 5)));
      };
      socket.onerror = () => setConnection('error');
      socket.onmessage = (event) => {
        if (typeof event.data !== 'string') { term.write(new Uint8Array(event.data as ArrayBuffer)); return; }
        try {
          const control = JSON.parse(event.data) as { type?: string; from?: number; to?: number; next?: number; seq?: number; data?: string; reason?: string };
          if (control.type === 'open' && cursor === undefined && control.next !== undefined) cursor = control.next;
          if (control.type === 'chunk' && control.seq !== undefined && control.data !== undefined) {
            if (cursor !== undefined && control.seq < cursor) return;
            if (cursor !== undefined && control.seq > cursor) term.writeln(`\r\n[output unavailable: ${cursor}–${control.seq}]`);
            cursor = control.seq + 1;
            term.write(decode(control.data));
          }
          if (control.type === 'gap') {
            term.writeln(`\r\n[output unavailable: ${control.from}–${control.to}]`);
            if (control.to !== undefined) cursor = control.to + 1;
          }
          if (control.type === 'exit') {
            terminalExit = true;
            term.writeln(`\r\n[session exited${control.reason ? `: ${control.reason}` : ''}]`);
          }
        } catch { term.write(event.data); }
      };
    };
    connect();
    const input = term.onData((data) => { if (socket?.readyState === WebSocket.OPEN) socket.send(data); });
    const sendResize = () => {
      fit.fit();
      if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: 'resize', rows: term.rows, cols: term.cols }));
    };
    const observer = new ResizeObserver(sendResize);
    observer.observe(host.current);
    return () => { disposed = true; window.clearTimeout(retryTimer); observer.disconnect(); input.dispose(); socket?.close(); term.dispose(); };
  }, [api, workspace, session, replayMinutes, initialNext]);

  return <section class="terminal-panel" aria-label={`Terminal session ${session}`}>
    <div class="terminal-bar"><span class={`live-dot ${connection}`} /> <code>{session}</code><span>{connection}</span></div>
    <div class="terminal" ref={host} />
  </section>;
}
