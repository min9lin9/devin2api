# Panel asset provenance and licenses

The files in this directory are the admin panel of devin2api, copied
verbatim from the upstream Go implementation
(`internal/dashboard/static/`, github.com/WncFht/devin2api, MIT License)
except where noted below. The panel itself is part of devin2api and is
covered by the repository's MIT `LICENSE`.

The only intentional deviation from upstream is runtime-metric rendering:
`js/tab-system.js` and the version-tag helper in `js/core.js` render the
approved Rust diagnostics (process RSS/CPU, `runtime: "rust"` marker) in
place of Go-only goroutine/heap/GC counters. No design or chart-library
changes.

## Bundled third-party components

- `echarts.min.js` — custom esbuild bundle of Apache ECharts 5.6.0
  (Apache License 2.0, copyright Baidu), zrender (BSD-3-Clause, copyright
  Baidu) and tslib (0BSD, copyright Microsoft), built by
  `scripts/build-echarts.sh` upstream. The bundled license texts are
  preserved at the end of the file itself.
- `js/vendor/morphdom.js` — morphdom (MIT License, copyright Patrick
  Steele-Idem), https://github.com/patrick-steele-idem/morphdom.
