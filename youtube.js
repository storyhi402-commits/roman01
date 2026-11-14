import { Readable } from 'node:stream';
import crypto from 'node:crypto';

const yt = new Map();
let drive = { token: '', at: 0 };
const bearer = (req) => (req.get('authorization') || '').replace(/^Bearer\s+/i, '').trim();

async function runUpload(job, ytToken, dToken, p) {
  const auth = { Authorization: 'Bearer ' + dToken };
  const fid = encodeURIComponent(p.video_file_id);
  const meta = await fetch(`https://www.googleapis.com/drive/v3/files/${fid}?fields=size,mimeType,name&supportsAllDrives=true`, { headers: auth });
  if (!meta.ok) throw new Error('drive meta ' + meta.status + ' ' + (await meta.text()).slice(0, 300));
  const { size } = await meta.json();
  const dl = await fetch(`https://www.googleapis.com/drive/v3/files/${fid}?alt=media&supportsAllDrives=true`, { headers: auth });
  if (!dl.ok || !dl.body) throw new Error('drive download ' + dl.status);
  const tags = String(p.tags || '').split(',').map((t) => t.trim()).filter(Boolean);
  const body = {
    snippet: { title: String(p.title || '').slice(0, 100), description: String(p.description || '').slice(0, 5000), tags, categoryId: String(p.category_id || '22') },
    status: { privacyStatus: p.privacy || 'private', selfDeclaredMadeForKids: false },
  };
  const init = await fetch('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + ytToken, 'Content-Type': 'application/json; charset=UTF-8', 'X-Upload-Content-Length': String(size), 'X-Upload-Content-Type': 'video/mp4' },
    body: JSON.stringify(body),
  });
  if (!init.ok) { dl.body.cancel().catch(() => {}); throw new Error('youtube init ' + init.status + ' ' + (await init.text()).slice(0, 500)); }
  const loc = init.headers.get('location');
  job.stage = 'uploading';
  const up = await fetch(loc, { method: 'PUT', headers: { 'Content-Length': String(size), 'Content-Type': 'video/mp4' }, body: Readable.fromWeb(dl.body), duplex: 'half' });
  const text = await up.text();
  if (!up.ok) throw new Error('youtube upload ' + up.status + ' ' + text.slice(0, 500));
  return JSON.parse(text);
}

export function registerYoutube(app) {
  app.post('/relay/drive-token', (req, res) => {
    const t = bearer(req);
    if (!t) return res.status(400).json({ error: 'no bearer token' });
    drive = { token: t, at: Date.now() };
    res.json({ ok: true });
  });

  app.post('/youtube-upload', (req, res) => {
    const t = bearer(req);
    const p = req.body || {};
    if (!t) return res.status(401).json({ error: 'no youtube bearer token' });
    if (!drive.token || Date.now() - drive.at > 50 * 60 * 1000) return res.status(409).json({ error: 'drive token missing or stale, call /relay/drive-token first' });
    if (!p.video_file_id || !p.title) return res.status(400).json({ error: 'video_file_id and title are required' });
    const id = crypto.randomUUID();
    const job = { status: 'running', stage: 'starting', startedAt: new Date().toISOString() };
    yt.set(id, job);
    res.status(202).json({ job_id: id, status: 'running' });
    runUpload(job, t, drive.token, p)
      .then((r) => {
        job.status = 'done';
        job.video_id = r.id;
        job.video_url = 'https://www.youtube.com/watch?v=' + r.id;
        job.upload_status = r.status && r.status.uploadStatus;
        job.privacy = r.status && r.status.privacyStatus;
      })
      .catch((e) => {
        job.status = 'error';
        job.error = String(e.message).slice(0, 1500);
        console.error('[youtube] job=' + id + ' error:', e);
      })
      .finally(() => {
        job.finishedAt = new Date().toISOString();
        setTimeout(() => yt.delete(id), 6 * 60 * 60 * 1000).unref();
      });
  });

  app.get('/youtube-upload/:id', (req, res) => {
    const j = yt.get(req.params.id);
    if (!j) return res.status(404).json({ error: 'unknown job' });
    res.json({ job_id: req.params.id, ...j });
  });
}
