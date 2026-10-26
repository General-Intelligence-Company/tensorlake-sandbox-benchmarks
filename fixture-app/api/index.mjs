import escapeHtml from 'escape-html';
export const greeting = (name) =>
  `Hello, ${escapeHtml(name.trim() || 'exe.dev')}!`;
export default function handler(req, res) {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/health') {
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ ok: true, provider: 'exe.dev' }));
    return;
  }
  res.setHeader('content-type', 'text/html');
  res.end(
    `<!doctype html><html><head><title>exe.dev retry workflow verified</title></head><body style="font:20px system-ui;max-width:700px;margin:80px auto;background:#f7f7f0;color:#173d32"><h1>exe.dev retry workflow verified</h1><p id="provider">Provider: exe.dev</p><p>Sandbox benchmark fixture</p><p id="greeting">${greeting(url.searchParams.get('name') || '')}</p><form><label>Your name <input name="name" placeholder="Name"></label><button>Greet</button></form><button id="counter" onclick="this.textContent='Clicks: '+(++window.clicks)">Clicks: 0</button><script>window.clicks=0</script></body></html>`,
  );
}
