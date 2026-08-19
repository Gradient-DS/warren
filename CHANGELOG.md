# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.4] — 2026-08-19

### Fixed

- `ResultDoc.created_at` is stored as a BSON `Date`, not an ISO string; a TTL
  index over a string field is inert. MongoDB's TTL monitor deletes only BSON
  Dates and skips every other type without logging a word, so a retention
  backstop declared over `created_at` deleted nothing. Nothing reads the field.
  Rows written before 0.2.4 keep their ISO string and are not expired by a TTL
  index; `ResultDoc` still parses them into a `datetime` on read.
- `RedisDictCache` renders `datetime` values as ISO 8601 text instead of
  refusing them. `DefaultResultsStore` caches the same dict it writes to
  MongoDB, so without this the cache write raised a `TypeError` that the store
  swallows as a log line — the results cache would have gone quietly dead.
  Cached dicts therefore carry `created_at` as ISO text; models that declare a
  `datetime` field parse it back on validation.

### Removed

- `warren.storage.utils.current_time_str` — `created_at` was its only caller.
