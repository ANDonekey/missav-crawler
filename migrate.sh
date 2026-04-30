#!/bin/bash
set -e

DB_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/missav?sslmode=disable}"
SQLITE_DB="data.db"

echo "=== Creating tables in Postgres ==="
psql "$DB_URL" <<'SQL'
DROP TABLE IF EXISTS streams;
DROP TABLE IF EXISTS crawl_jobs;
DROP TABLE IF EXISTS videos;

CREATE TABLE videos (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL UNIQUE,
    title TEXT,
    duration TEXT,
    section TEXT,
    description TEXT,
    cover_url TEXT,
    release_date TEXT,
    duration_seconds TEXT,
    actors TEXT,
    genres TEXT,
    maker TEXT,
    director TEXT,
    tags TEXT,
    detail_status TEXT NOT NULL DEFAULT 'pending',
    stream_status TEXT NOT NULL DEFAULT 'pending',
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_crawled_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE streams (
    id BIGSERIAL PRIMARY KEY,
    video_code TEXT NOT NULL,
    video_url TEXT NOT NULL,
    stream_type TEXT,
    m3u8_url TEXT NOT NULL UNIQUE,
    m3u8_path TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (video_code) REFERENCES videos (code)
);

CREATE TABLE crawl_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    url TEXT NOT NULL,
    video_code TEXT,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    last_error TEXT,
    scheduled_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (job_type, url)
);
SQL

echo "=== Migrating videos ==="
sqlite3 "$SQLITE_DB" ".mode insert videos" "SELECT * FROM videos;" | psql "$DB_URL"

echo "=== Migrating streams ==="
sqlite3 "$SQLITE_DB" ".mode insert streams" "SELECT * FROM streams;" | psql "$DB_URL"

echo "=== Migrating crawl_jobs ==="
sqlite3 "$SQLITE_DB" ".mode insert crawl_jobs" "SELECT * FROM crawl_jobs;" | psql "$DB_URL"

echo "=== Updating sequences ==="
psql "$DB_URL" <<'SQL'
SELECT setval('videos_id_seq', COALESCE((SELECT MAX(id) FROM videos), 1));
SELECT setval('streams_id_seq', COALESCE((SELECT MAX(id) FROM streams), 1));
SELECT setval('crawl_jobs_id_seq', COALESCE((SELECT MAX(id) FROM crawl_jobs), 1));
SQL

echo "=== Verifying ==="
psql "$DB_URL" -c "SELECT 'videos:', count(*) FROM videos;"
psql "$DB_URL" -c "SELECT 'streams:', count(*) FROM streams;"
psql "$DB_URL" -c "SELECT 'crawl_jobs:', count(*) FROM crawl_jobs;"

echo "=== Done ==="
