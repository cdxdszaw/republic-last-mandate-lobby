// Republic: Last Mandate — presence only. Never accepts campaign state.
import http from 'node:http';
import { realpathSync } from 'node:fs';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';
export const clean = (s, max) => typeof s === 'string' ? [...s.normalize('NFC').replace(/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/gu, ' ').replace(/\s+/gu, ' ').trim()].slice(0, max).join('') : '';
const remoteIP = s => (s || '').replace(/^::ffff:/, '');
export function createLobby(options = {}) {
  const host = options.host ?? process.env.HOST ?? '127.0.0.1';
  const port = Number(options.port ?? process.env.PORT ?? 8080);
  const maxClients = Math.max(1,Math.min(100,Number(options.maxClients ?? process.env.MAX_CLIENTS ?? 100)));
  const trustProxy = options.trustProxy ?? process.env.TRUST_PROXY === '1';
  const adminToken = options.adminToken ?? process.env.ADMIN_TOKEN ?? '';
  const heartbeatMs = options.heartbeatMs ?? 10000;
  const helloTimeoutMs = options.helloTimeoutMs ?? 7000;
  const idleTimeoutMs = options.idleTimeoutMs ?? 35000;
  const peers = new Map(), history = [], attempts = new Map(), bans = new Map();
  let sequence = 0;
  const send = (socket, value) => {
    if (socket.readyState !== WebSocket.OPEN) return;
    if (socket.bufferedAmount > 128 * 1024) { socket.terminate(); return; }
    socket.send(JSON.stringify(value));
  };
  const broadcast = value => { for (const [s, p] of peers) if (p.joined) send(s, value); };
  const roster = () => [...peers.values()].filter(p => p.joined).map(p => ({id:p.id,nick:p.nick}));
  const publishRoster = () => broadcast({type:'roster',players:roster()});
  const fail = (s, text) => send(s,{type:'error',text});
  const authorized = req => {
    if (!adminToken || adminToken.length < 24) return false;
    const a = Buffer.from(req.headers.authorization || ''), b = Buffer.from('Bearer '+adminToken);
    return a.length === b.length && timingSafeEqual(a,b);
  };
  const server = http.createServer((req,res) => {
    res.setHeader('Cache-Control','no-store'); res.setHeader('Content-Type','application/json; charset=utf-8');
    if (req.method === 'GET' && req.url === '/health') { res.writeHead(200); res.end(JSON.stringify({ok:true,version:1,online:roster().length})); return; }
    const match = /^\/admin\/clients\/([a-f0-9-]+)$/.exec(req.url || '');
    if (req.method === 'DELETE' && match && authorized(req)) {
      for (const [s,p] of peers) if (p.id === match[1]) { bans.set(p.ip, Date.now()+3600000); send(s,{type:'error',text:'Оператор отключил соединение на один час.'}); s.close(1008,'Removed by operator'); }
      res.writeHead(204); res.end(); return;
    }
    // Names and session IDs are available only to an authenticated operator here.
    if (req.method === 'GET' && req.url === '/admin/clients' && authorized(req)) { res.writeHead(200); res.end(JSON.stringify(roster())); return; }
    res.writeHead(404); res.end('{"error":"not_found"}');
  });
  server.headersTimeout = 8000; server.requestTimeout = 8000;
  const wss = new WebSocketServer({noServer:true,maxPayload:4096,perMessageDeflate:false});
  const reject = (socket,code) => { socket.end(`HTTP/1.1 ${code} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`); };
  server.on('upgrade',(req,socket,head) => {
    if (req.url !== '/lobby') { reject(socket,404); return; }
    let ip = remoteIP(req.socket.remoteAddress);
    if (trustProxy) ip = remoteIP(String(req.headers['x-forwarded-for'] || ip).split(',').at(-1).trim());
    const now = Date.now();
    if ((bans.get(ip)||0) > now) { reject(socket,403); return; }
    const a = attempts.get(ip) || {time:now,count:0};
    if (now-a.time > 60000) { a.time=now; a.count=0; }
    a.count++; attempts.set(ip,a);
    if (a.count > 30 || peers.size >= maxClients || [...peers.values()].filter(p=>p.ip===ip).length>=16) { reject(socket,429); return; }
    wss.handleUpgrade(req,socket,head,ws=>wss.emit('connection',ws,ip));
  });
  wss.on('connection',(socket,ip) => {
    const p = {id:randomUUID(),nick:'',ip,joined:false,lastPong:Date.now(),tokens:5,tokenTime:Date.now(),packets:0,window:Date.now()};
    peers.set(socket,p);
    const helloTimer = setTimeout(()=>socket.close(1008,'Handshake timeout'),helloTimeoutMs); helloTimer.unref();
    socket.on('error',()=>{}); // no payload or IP is written to logs
    socket.on('message',(data,binary) => {
      const now=Date.now();
      if (now-p.window>10000) { p.window=now; p.packets=0; }
      if (++p.packets>40 || binary) { socket.close(1008,'Protocol limit'); return; }
      let m; try { m=JSON.parse(data.toString()); } catch { socket.close(1008,'Invalid JSON'); return; }
      if (!m || typeof m !== 'object' || Array.isArray(m)) { socket.close(1008,'Invalid message'); return; }
      if (m.type === 'pong' && p.joined) { p.lastPong=now; return; }
      if (!p.joined) {
        if (m.type!=='hello' || m.version!==1 || typeof m.nick!=='string' || [...m.nick].length>24) { fail(socket,'Несовместимый протокол или слишком длинный псевдоним.'); socket.close(1008,'Invalid hello'); return; }
        const nick=clean(m.nick,24);
        if ([...nick].length<2) { fail(socket,'Псевдоним должен содержать от 2 до 24 символов.'); socket.close(1008,'Invalid nickname'); return; }
        if ([...peers.values()].some(other=>other.joined && other.nick.toLocaleLowerCase('ru')===nick.toLocaleLowerCase('ru'))) { fail(socket,'Этот псевдоним уже занят. Выберите другой.'); socket.close(1008,'Nickname in use'); return; }
        clearTimeout(helloTimer); p.nick=nick; p.joined=true;
        send(socket,{type:'welcome',version:1,id:p.id,players:roster(),history});
        publishRoster(); broadcast({type:'notice',text:`${nick} присоединился.`}); return;
      }
      if (m.type !== 'chat' || typeof m.text !== 'string') { fail(socket,'Неподдерживаемое сообщение.'); return; }
      if ([...m.text].length>400) { fail(socket,'Лимит сообщения — 400 символов.'); return; }
      const text=clean(m.text,400);
      if (!text) { fail(socket,'Пустое сообщение не отправлено.'); return; }
      p.tokens=Math.min(5,p.tokens+(now-p.tokenTime)/1250); p.tokenTime=now;
      if (p.tokens<1) { fail(socket,'Слишком быстро. Подождите пару секунд.'); return; }
      p.tokens--;
      const message={type:'chat',id:++sequence,from:p.id,nick:p.nick,text,time:now};
      history.push(message); if(history.length>50) history.shift(); broadcast(message);
    });
    socket.on('close',()=>{ clearTimeout(helloTimer); peers.delete(socket); if(p.joined){ publishRoster(); broadcast({type:'notice',text:`${p.nick} отключился.`}); } });
  });
  const timer = setInterval(()=>{
    const now=Date.now();
    for(const [s,p] of peers) if(p.joined){ if(now-p.lastPong>idleTimeoutMs) s.terminate(); else send(s,{type:'ping'}); }
    for(const [ip,a] of attempts) if(now-a.time>60000) attempts.delete(ip);
    for(const [ip,until] of bans) if(now>until) bans.delete(ip);
  },heartbeatMs); timer.unref();
  return {server,wss,roster,history,listen:()=>new Promise((resolve,reject)=>{server.once('error',reject);server.listen(port,host,()=>{server.removeListener('error',reject);resolve(server.address());});}),close:async()=>{clearInterval(timer);for(const s of peers.keys()) s.terminate();await new Promise(resolve=>wss.close(resolve));if(server.listening)await new Promise(resolve=>server.close(resolve));}};
}
if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  const lobby=createLobby();
  lobby.listen().then(address=>console.log(`Republic lobby 1.6 listening on ${address.address}:${address.port}; path /lobby`)).catch(e=>{console.error('Startup failed:',e.code || e.message);process.exit(1);});
  for (const signal of ['SIGINT','SIGTERM']) process.on(signal,async()=>{await lobby.close();process.exit(0);});
}
