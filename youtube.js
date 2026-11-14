import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createReadStream, createWriteStream, promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

const yt = new Map();
const bearer = (req) => (req.get('authorization') || '').replace(/^Bearer\s+/i, '').trim();

async function runUpload(job, ytToken, p) {
  const work = await fs.mkdtemp(path.join(os.tmpdir(), 'yt-'));
  try {
    const fid = encodeURIComponent(p.video_file_id);
    const url = `https://drive.usercontent.google.com/download?id=${fid}&export=download&confirm=t`;
    job.stage = 'downloading';
    const dl = await fetch(url, { redirect: 'follow' });
    if (!dl.ok || !dl.body) throw new Error('drive download failed ' + dl.status);
    if ((dl.headers.get('content-type') || '').includes('text/html')) throw new Error('drive returned a web page instead of the file (is the file shared by link?)');
    const file = path.join(work, 'video.mp4');
    await pipeline(Readable.fromWeb(dl.body), createWriteStream(file));
    const size = (await fs.stat(file)).size;
    job.size = size;
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
    if (!init.ok) throw new Error('youtube init ' + init.status + ' ' + (await init.text()).slice(0, 500));
    const loc = init.headers.get('location');
    job.stage = 'uploading';
    const up = await fetch(loc, { method: 'PUT', headers: { 'Content-Length': String(size), 'Content-Type': 'video/mp4' }, body: createReadStream(file), duplex: 'half' });
    const text = await up.text();
    if (!up.ok) throw new Error('youtube upload ' + up.status + ' ' + text.slice(0, 500));
    return JSON.parse(text);
  } finally {
    fs.rm(work, { recursive: true, force: true }).catch(() => {});
  }
}

export function registerYoutube(app) {
  app.post('/youtube-upload', (req, res) => {
    const t = bearer(req);
    const p = req.body || {};
    if (!t) return res.status(401).json({ error: 'no youtube bearer token' });
    if (!p.video_file_id || !p.title) return res.status(400).json({ error: 'video_file_id and title are required' });
    const id = crypto.randomUUID();
    const job = { status: 'running', stage: 'starting', startedAt: new Date().toISOString() };
    yt.set(id, job);
    res.status(202).json({ job_id: id, status: 'running' });
    runUpload(job, t, p)
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
