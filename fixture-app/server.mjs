import http from 'node:http';
import handler from './api/index.mjs';
http
  .createServer(handler)
  .listen(8000, '0.0.0.0', () => console.log('exe app listening on 8000'));
