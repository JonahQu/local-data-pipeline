#!/usr/bin/env python3
"""Local staging of existing VM exports. No collector, credentials, or network."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys

ROOT = Path(os.environ.get('LOCAL_DATA_ROOT', str(Path(__file__).resolve().parents[1] / 'runtime'))).resolve()
SOURCE = Path(os.environ.get('LOCAL_DATA_SOURCE', str(Path(__file__).resolve().parents[1] / 'examples/input'))).resolve()
NAMESPACE = os.environ.get('LOCAL_DATA_NAMESPACE', 'local-json-export-v1')
FIELDS = ('id', 'from', 'isSender', 'type', 'datetime', 'content', 'img',
          'name', 'senderId', 'sessionId', 'localId', 'serverId', 'createTime',
          'recoveredFromRecallJournal')


def now():
    return datetime.now(timezone.utc).isoformat()


def packed(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(value if isinstance(value, bytes) else value.encode()).hexdigest()


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('w', encoding='utf-8') as f:
        f.write(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
        f.flush()
        os.fsync(f.fileno())
    temp.replace(path)


@contextmanager
def writer(root):
    os.umask(0o077)
    (root / 'state').mkdir(parents=True, exist_ok=True, mode=0o700)
    with (root / 'state/writer.lock').open('a') as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def connect(root, readonly=False):
    path = root / 'data/staging.sqlite3'
    if readonly:
        db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
        db.execute('PRAGMA query_only=ON')
    else:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        db = sqlite3.connect(path)
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        db.executescript('''
        CREATE TABLE IF NOT EXISTS messages(
          message_id TEXT PRIMARY KEY, namespace TEXT NOT NULL,
          conversation_id TEXT NOT NULL, source_message_id TEXT NOT NULL,
          identity_quality TEXT NOT NULL, event_time INTEGER NOT NULL,
          content_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
          source_file TEXT NOT NULL, source_row INTEGER NOT NULL,
          input_sha256 TEXT NOT NULL, first_imported_at TEXT NOT NULL,
          last_imported_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS changes(
          seq INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT NOT NULL,
          operation TEXT NOT NULL, content_hash TEXT NOT NULL,
          payload_json TEXT NOT NULL, source_file TEXT NOT NULL,
          source_row INTEGER NOT NULL, input_sha256 TEXT NOT NULL,
          imported_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS input_state(
          source_file TEXT PRIMARY KEY, input_sha256 TEXT NOT NULL,
          rows_seen INTEGER NOT NULL, imported_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS consumers(
          consumer TEXT PRIMARY KEY, through_seq INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS summaries(
          summary_id TEXT PRIMARY KEY, from_seq INTEGER NOT NULL,
          through_seq INTEGER NOT NULL, payload_json TEXT NOT NULL,
          sha256 TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS runs(
          run_id TEXT PRIMARY KEY, finished_at TEXT NOT NULL,
          result_json TEXT NOT NULL);
        ''')
    db.row_factory = sqlite3.Row
    return db


def normalized(message, conversation):
    if not isinstance(message, dict):
        raise ValueError('message_not_object')
    payload = {k: message[k] for k in FIELDS if k in message}
    stamp = payload.get('createTime')
    if not isinstance(stamp, int) or isinstance(stamp, bool) or stamp < 0:
        raise ValueError('invalid_event_time')
    if not isinstance(payload.get('content', ''), str):
        raise ValueError('content_not_text')
    server = str(payload.get('serverId', '')).strip()
    native = str(payload.get('id', '')).strip()
    local = payload.get('localId')
    if server not in ('', '0', 'None', '-1'):
        source_id, quality = 'server:' + server, 'server_id'
    elif native not in ('', '0', 'None'):
        source_id, quality = 'native:' + native + ':' + str(stamp), 'source_local'
    elif local is not None:
        source_id, quality = 'local:' + str(local) + ':' + str(stamp), 'source_local'
    else:
        # Weak identities stay source scoped. Never merge across accounts/hosts.
        source_id = 'fallback:' + digest(packed([payload.get('senderId'), stamp,
                      payload.get('type'), payload.get('content'), payload.get('img')]))
        quality = 'weak_fingerprint'
    mid = digest(packed([NAMESPACE, conversation, source_id]))
    return mid, source_id, quality, stamp, payload


def ingest(root=ROOT, source=SOURCE):
    """One bounded pass over existing files; source files are never written."""
    source = Path(source)
    if not source.is_dir() or source.is_symlink():
        raise ValueError('source_missing_or_symlink')
    with writer(root):
        db = connect(root)
        result = {'schema_version': 1, 'run_id': now(), 'source': 'json_export',
                  'new': 0, 'updated': 0, 'unchanged': 0, 'files_skipped': 0,
                  'files_imported': 0, 'failures': [], 'collector_started': False}
        for path in sorted(source.iterdir()):
            if not re.fullmatch(r'[a-f0-9]{32}\.json', path.name):
                continue
            counts = {'new': 0, 'updated': 0, 'unchanged': 0}
            try:
                if path.is_symlink() or path.stat().st_size > 16 * 1024 * 1024:
                    raise ValueError('unsafe_or_oversized_input')
                raw = path.read_bytes()  # Existing producer publishes atomic replacements.
                sha = digest(raw)
                previous = db.execute('SELECT input_sha256 FROM input_state WHERE source_file=?', (str(path),)).fetchone()
                if previous and previous[0] == sha:
                    result['files_skipped'] += 1
                    continue
                doc = json.loads(raw.decode('utf-8-sig'))
                conversation = doc['conversationId']
                records = doc['messages']
                if conversation != path.stem or not isinstance(records, list) or len(records) > 20000:
                    raise ValueError('invalid_archive_shape')
                imported = now()
                with db:
                    for row, message in enumerate(records):
                        mid, sid, quality, stamp, payload = normalized(message, conversation)
                        body = packed(payload)
                        content_hash = digest(body)
                        old = db.execute('SELECT content_hash FROM messages WHERE message_id=?', (mid,)).fetchone()
                        if old and old[0] == content_hash:
                            counts['unchanged'] += 1
                            continue
                        operation = 'updated' if old else 'new'
                        db.execute('''INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                          ON CONFLICT(message_id) DO UPDATE SET
                          content_hash=excluded.content_hash,payload_json=excluded.payload_json,
                          event_time=excluded.event_time,source_file=excluded.source_file,
                          source_row=excluded.source_row,input_sha256=excluded.input_sha256,
                          last_imported_at=excluded.last_imported_at''',
                          (mid, NAMESPACE, conversation, sid, quality, stamp, content_hash,
                           body, str(path), row, sha, imported, imported))
                        db.execute('INSERT INTO changes(message_id,operation,content_hash,payload_json,source_file,source_row,input_sha256,imported_at) VALUES(?,?,?,?,?,?,?,?)',
                                   (mid, operation, content_hash, body, str(path), row, sha, imported))
                        counts[operation] += 1
                    db.execute('INSERT OR REPLACE INTO input_state VALUES(?,?,?,?)', (str(path), sha, len(records), imported))
                for k, v in counts.items():
                    result[k] += v
                result['files_imported'] += 1
            except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
                db.rollback()
                # Do not log exception text: parser errors may contain chat content.
                result['failures'].append({'file': path.name, 'category': type(exc).__name__})
        result['state'] = 'partial' if result['failures'] else 'succeeded'
        result['message_count'] = db.execute('SELECT count(*) FROM messages').fetchone()[0]
        result['change_count'] = db.execute('SELECT count(*) FROM changes').fetchone()[0]
        result['input_file_count'] = db.execute('SELECT count(*) FROM input_state').fetchone()[0]
        result['integrity_check'] = db.execute('PRAGMA quick_check').fetchone()[0]
        result['finished_at'] = now()
        with db:
            db.execute('INSERT INTO runs VALUES(?,?,?)', (result['run_id'], result['finished_at'], packed(result)))
        db.close()
        atomic(root / 'state/ingest.json', result)
        atomic(root / 'evidence' / ('ingest-' + digest(result['run_id'])[:16] + '.json'), result)
        return result


def summarize(root=ROOT, limit=50):
    """Local extractive digest, never an AI inference or external API call."""
    if not 1 <= limit <= 100:
        raise ValueError('limit_out_of_range')
    with writer(root):
        db = connect(root)
        cursor = db.execute("SELECT through_seq FROM consumers WHERE consumer='summary'").fetchone()
        after = cursor[0] if cursor else 0
        rows = db.execute('SELECT * FROM changes WHERE seq>? ORDER BY seq LIMIT ?', (after, limit)).fetchall()
        if not rows:
            db.close()
            return {'state': 'no_new_changes', 'through_seq': after, 'summary_created': False}
        until = rows[-1]['seq']
        entries = []
        for row in rows:
            p = json.loads(row['payload_json'])
            entries.append({'seq': row['seq'], 'message_id': row['message_id'],
                'change': row['operation'], 'event_time': p['createTime'],
                'type': p.get('type', 'unknown'), 'excerpt': p.get('content', '')[:240],
                'content_hash': row['content_hash'], 'source_file': row['source_file'],
                'source_row': row['source_row'], 'input_sha256': row['input_sha256']})
        summary_id = 'vm-' + str(after + 1) + '-' + str(until)
        report = {'schema_version': 1, 'summary_id': summary_id, 'created_at': now(),
                  'from_seq': after + 1, 'through_seq': until, 'record_count': len(rows),
                  'method': 'local_extractive_v1', 'coverage': 'provided_local_json_files_only',
                  'semantic_inference': False, 'entries': entries}
        body = packed(report)
        sha = digest(body)
        with db:
            db.execute('INSERT INTO summaries VALUES(?,?,?,?,?,?)', (summary_id, after + 1, until, body, sha, now()))
            db.execute("INSERT OR REPLACE INTO consumers VALUES('summary',?)", (until,))
        db.close()
        # DB payload is authoritative; a crash here is repaired by publish().
        publish(root)
        return {'state': 'succeeded', 'summary_id': summary_id, 'through_seq': until,
                'records': len(rows), 'sha256': sha, 'summary_created': True}


def publish(root=ROOT):
    """Rebuild local output from committed DB, without advancing any cursor."""
    db = connect(root, True)
    summaries = db.execute('SELECT * FROM summaries ORDER BY through_seq').fetchall()
    for row in summaries:
        atomic(root / 'summaries' / (row['summary_id'] + '.json'), json.loads(row['payload_json']))
    manifest = {'schema_version': 1, 'generated_at': now(), 'delivery': 'local_only',
                'dot_readable': 'unverified', 'summaries': [
                    {'summary_id': r['summary_id'], 'through_seq': r['through_seq'],
                     'sha256': r['sha256'], 'path': str(root / 'summaries' / (r['summary_id'] + '.json'))}
                    for r in summaries]}
    atomic(root / 'outbox/manifest.json', manifest)
    db.close()
    return {'state': 'published_locally', 'summaries': len(summaries), 'dot_readable': 'unverified'}


def read(root=ROOT, after=0, limit=20, metadata_only=False):
    if after < 0 or not 1 <= limit <= 100:
        raise ValueError('bounds_invalid')
    db = connect(root, True)
    rows = db.execute('SELECT * FROM changes WHERE seq>? ORDER BY seq LIMIT ?', (after, limit)).fetchall()
    maximum = db.execute('SELECT coalesce(max(seq),0) FROM changes').fetchone()[0]
    result = {'schema_version': 1, 'reader': 'local_cli', 'dot_verified': False,
              'after_seq': after, 'through_seq': rows[-1]['seq'] if rows else after,
              'has_more': bool(rows and rows[-1]['seq'] < maximum), 'records': []}
    for row in rows:
        record = {k: row[k] for k in ('seq', 'message_id', 'operation', 'content_hash', 'input_sha256', 'source_row')}
        if not metadata_only:
            record['message'] = json.loads(row['payload_json'])
            record['source_file'] = row['source_file']
        result['records'].append(record)
    db.close()
    return result


def main():
    os.umask(0o077)
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('action', choices=['ingest', 'summarize', 'publish', 'read'])
    ap.add_argument('--after', type=int, default=0)
    ap.add_argument('--limit', type=int, default=20)
    ap.add_argument('--metadata-only', action='store_true')
    ap.add_argument('--root', type=Path, default=ROOT)
    ap.add_argument('--source', type=Path, default=SOURCE)
    args = ap.parse_args()
    args.root = args.root.resolve()
    if args.action == 'ingest':
        result = ingest(args.root, args.source)
    elif args.action == 'summarize':
        result = summarize(args.root, limit=args.limit)
    elif args.action == 'publish':
        with writer(args.root):
            result = publish(args.root)
    else:
        result = read(args.root, after=args.after, limit=args.limit, metadata_only=args.metadata_only)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result.get('state') == 'partial':
        sys.exit(2)


if __name__ == '__main__':
    main()
