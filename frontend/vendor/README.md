# Vendored front-end libraries

Served from our own origin instead of a public CDN. The map is the core of the
site and it used to go down whenever unpkg did; a third-party outage is not a
good reason for the product to stop working. Same-origin also means one less
party seeing every visitor's IP, and no extra DNS and TLS handshake before the
map can start.

nginx serves everything here with a 30-day immutable cache, so the filenames
must stay version-pinned in this README rather than in the path.

| File                | Upstream                                                              | Version |
|---------------------|-----------------------------------------------------------------------|---------|
| maplibre-gl.js      | https://unpkg.com/maplibre-gl@5.2.0/dist/maplibre-gl.js                | 5.2.0   |
| maplibre-gl.css     | https://unpkg.com/maplibre-gl@5.2.0/dist/maplibre-gl.css               | 5.2.0   |
| chart.umd.min.js    | https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js      | 4.4.4   |

Licences: MapLibre GL JS is 3-Clause BSD, Chart.js is MIT. Both permit
redistribution with the licence text, which is retained in the file headers.

To upgrade, download the new file over the old one, bump the version here, and
load /map with a hard refresh to confirm the map draws and a neighbourhood
panel renders its score chart. The basemap tiles stay remote: those are a
hosted service, not a library.
