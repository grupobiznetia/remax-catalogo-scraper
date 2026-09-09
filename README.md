# remax-catalogo-scraper

Baja el catálogo público de RE/MAX Córdoba (API `api-ar.redremax.com`) y lo envía a
la tabla `market_properties` de Supabase (proyecto Real Coequiper) vía la edge
function `market-ingest`.

Corre solo, todas las noches, con GitHub Actions. No necesita servidor propio ni la PC prendida.

## Setup (una vez)

1. Crear este repo en GitHub (público → minutos de Actions ilimitados).
2. Repo → **Settings → Secrets and variables → Actions → New repository secret**:

   | Nombre | Valor |
   |---|---|
   | `MARKET_INGEST_URL` | `https://jbuyrufuaxlajwxdbbub.supabase.co/functions/v1/market-ingest` |
   | `MARKET_INGEST_SECRET` | el mismo valor del secret `MARKET_INGEST_SECRET` de Supabase |

3. Push de estos archivos a la rama por defecto.
4. Pestaña **Actions** → workflow "scrape" → **Run workflow** para la primera corrida completa (~20-25 min).

Después corre solo a las 02:00 AM Argentina (cron `0 5 * * *` UTC).

## Config (en `.github/workflows/scrape.yml`)

- `OPERATION_IDS` — `1` = venta. `1,2` para sumar alquileres.
- `MAX_PROPS` — `0` = sin tope. Un número para limitar (pruebas).

## Correr a mano en local

```bash
pip install -r requirements.txt
OPERATION_IDS=1 MAX_PROPS=50 \
MARKET_INGEST_URL=https://jbuyrufuaxlajwxdbbub.supabase.co/functions/v1/market-ingest \
MARKET_INGEST_SECRET=... \
python scrape.py
```

Sin `MARKET_INGEST_URL`/`SECRET` solo escribe `remax_cordoba_properties.ndjson`, no envía nada.

## Qué hace `market-ingest` (del lado Supabase)

- Upsert por `source` + `source_id` (internalId de RE/MAX).
- `price_usd` normalizado (ARS→USD con el dólar del día).
- Historial de precio si cambió precio/moneda/estado.
- Altas/bajas: lo que no vuelve en el snapshot pasa a `is_active = false`.
- Invalida el `embedding` si cambió `title` / `description` / `features`.

## Notas

- El scraper usa solo endpoints públicos de RE/MAX, con pausa entre requests. No usa login ni evade nada.
- La búsqueda semántica (embeddings) está pausada por costo; `market_search` funciona con filtros + geo + full-text.
