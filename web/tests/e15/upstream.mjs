// Two plain-HTTP upstreams for the E15 credential-boundary fixture.
//
//   allowed  — the only host the b_e15 binding covers. The parked approve-mode
//              request is released to it once the browser commits a decision.
//   foreign  — a host the binding does NOT cover. If the broker ever forwarded
//              a placeholder there, this server would record the request and
//              the Authorization header it carried. It must stay empty.
//
// Each server answers GET /__hits with everything it saw, so Playwright can
// assert on delivery rather than only on what the console rendered.
import { createServer } from 'node:http';

function serve(port, label) {
  const hits = [];
  const server = createServer((request, response) => {
    if (request.url === '/__hits') {
      const body = JSON.stringify({ label, hits });
      response.writeHead(200, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
      response.end(body);
      return;
    }
    hits.push({
      method: request.method,
      url: request.url,
      authorization: request.headers.authorization ?? '',
      headers: JSON.stringify(request.headers),
    });
    response.writeHead(200, { 'content-type': 'text/plain' });
    response.end('upstream-ok');
  });
  server.listen(port, '127.0.0.1', () => process.stdout.write(`e15 upstream ${label} listening on ${port}\n`));
  return server;
}

const allowed = Number(process.argv[2]);
const foreign = Number(process.argv[3]);
if (!Number.isInteger(allowed) || !Number.isInteger(foreign)) {
  process.stderr.write('usage: upstream.mjs <allowedPort> <foreignPort>\n');
  process.exit(2);
}
serve(allowed, 'allowed');
serve(foreign, 'foreign');
