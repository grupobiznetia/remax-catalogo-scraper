#!/usr/bin/env python3
"""
Scraper RE/MAX Córdoba - Extracción completa de propiedades
Corrección: todos los campos físicos se leen del detalle (findBySlug)
Bonus: apt_credit, in_private_community, pozo, financing, year_built, 
       dimension_uncovered, dimension_semicovered
"""

import os
import sys
import requests
import json
import time
import re
from datetime import datetime, timezone
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Config (env) ---
OPERATION_IDS = [int(x) for x in os.environ.get("OPERATION_IDS", "1").split(",") if x.strip()]  # 1=venta, 2=alquiler
MAX_PROPS = int(os.environ.get("MAX_PROPS", "0"))  # 0 = sin tope
INGEST_URL = os.environ.get("MARKET_INGEST_URL", "")
INGEST_SECRET = os.environ.get("MARKET_INGEST_SECRET", "")

# Polígono que cubre toda la provincia de Córdoba (26 puntos lng lat, del mapa de remax.com.ar).
# La API usa `withinarea`, no `locations` (ese id de barrio no sirve para toda la provincia).
_CORDOBA_POLY = (
    "location:-63.15355643373719 -34.33979540938583,-64.42797049623718 -34.33072343414419,"
    "-65.03221854311218 -34.07631024973466,-64.98827323061218 -33.11315676356412,"
    "-64.83446463686218 -32.346077548619775,-64.98827323061218 -32.27876093476629,"
    "-65.16405448061218 -32.27643878019716,-65.24095877748718 -32.06021888208314,"
    "-65.40300711733093 -31.981042772584676,-65.5938945685028 -31.63442918027538,"
    "-65.58702811342468 -31.367461841356093,-65.34944876772155 -30.567924166273414,"
    "-65.02947196108093 -30.058146651438143,-64.35861929994812 -30.363708116577303,"
    "-64.72872122865905 -30.153752310544725,-64.80562552553405 -29.66808333886819,"
    "-63.88826712709656 -29.390866142940748,-62.66878470522156 -30.00639770898353,"
    "-62.23482474428406 -30.310372086346813,-61.76241263490906 -30.66539560970916,"
    "-62.05904349428406 -31.530863702179722,-62.41884574037781 -32.032833918550246,"
    "-62.41905710521496 -32.60627600499042,-62.06749460521496 -32.878876202052474,"
    "-61.98509714427746 -33.192022493305664,-62.87498972240246 -34.37448014168884"
)
AREA_WITHINAREA = os.environ.get("AREA_WITHINAREA", _CORDOBA_POLY)
# El polígono es a mano y se pasa un poco a provincias vecinas; este filtro deja solo la provincia real.
ONLY_PROVINCIA = os.environ.get("ONLY_PROVINCIA", "Córdoba")  # vacío = sin filtro


INGEST_BATCH = int(os.environ.get("INGEST_BATCH", "300"))


def post_to_ingest(properties: List[Dict]) -> None:
    if not INGEST_URL or not INGEST_SECRET:
        logger.info("MARKET_INGEST_URL/SECRET no seteados — no se envía a Supabase")
        return
    run_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    total = len(properties)
    logger.info(f"Enviando {total} propiedades a {INGEST_URL} en tandas de {INGEST_BATCH} (run_ts={run_ts})")
    for i in range(0, max(total, 1), INGEST_BATCH):
        chunk = properties[i:i + INGEST_BATCH]
        is_last = (i + INGEST_BATCH) >= total
        payload = {
            "properties": chunk,
            "source": "remax",
            "run_ts": run_ts,
            "snapshot_complete": is_last,
        }
        r = requests.post(
            INGEST_URL,
            headers={"Content-Type": "application/json", "x-ingest-secret": INGEST_SECRET},
            data=json.dumps(payload).encode("utf-8"),
            timeout=120,
        )
        logger.info(f"  tanda {i // INGEST_BATCH + 1} ({len(chunk)} props, final={is_last}): "
                    f"HTTP {r.status_code} {r.text[:400]}")
        r.raise_for_status()
    logger.info("Ingest completo.")

class RemaxCordobaScraper:
    def __init__(self):
        self.base_url = "https://api-ar.redremax.com/remaxweb-ar/api"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
            "Origin": "https://www.remax.com.ar",
            "Referer": "https://www.remax.com.ar/"
        }
        self.scraped_at = datetime.utcnow().isoformat() + "Z"
        
        self.operation_map = {
            "sale": "venta",
            "rent": "alquiler",
        }
        
        self.status_map = {
            "active": "activa",
            "reserved": "reservada",
            "negotiation": "en_negociacion",
            "sold": "vendida",
            "rented": "alquilada",
            "inactive": "inactiva"
        }
        
        self.property_type_map = {
            "departamento_estandar": "departamento",
            "departamento_duplex": "departamento",
            "departamento_penthouse": "departamento",
            "departamento": "departamento",
            "casa": "casa",
            "ph": "ph",
            "lote": "lote",
            "terreno": "terreno",
            "local": "local",
            "oficina": "oficina",
            "galpon": "galpon",
            "deposito": "depósito",
            "chacra": "chacra",
            "estancia": "estancia",
            "local_comercial": "local",
            "oficina_comercial": "oficina",
            "terrenos_y_lotes": "lote",
            "casa_duplex": "duplex",
            "casa_triplex": "casa",
            "departamento_triplex": "departamento",
            "departamento_monoambiente": "departamento",
            "campo": "campo",
            "fondo_de_comercio": "local",
        }
        
        self.provincia_map = {
            "cordoba": "Córdoba",
            "córdoba": "Córdoba",
            "buenos aires": "Buenos Aires",
            "santa fe": "Santa Fe",
            "entre rios": "Entre Ríos",
            "entre ríos": "Entre Ríos",
        }
        
        self.partido_map = {
            "capital": "Capital",
            "punilla": "Punilla",
            "colon": "Colón",
            "colón": "Colón",
            "calamuchita": "Calamuchita",
            "santa maria": "Santa María",
            "santa maría": "Santa María",
            "san javier": "San Javier",
            "san alberto": "San Alberto",
            "rio segundo": "Río Segundo",
            "río segundo": "Río Segundo",
            "rio tercero": "Río Tercero",
            "totoral": "Totoral",
            "uncia": "Uncía",
            "uncía": "Uncía",
            "cruz del eje": "Cruz del Eje",
            "pocho": "Pocho",
            "san roque": "San Roque",
            "minas": "Minas",
            "isla verde": "Isla Verde",
            "general roca": "General Roca",
            "general san martin": "General San Martín",
            "tulumamba": "Tulumamba",
            "rio primero": "Río Primero",
            "rio cuarto": "Río Cuarto",
            "juarez celman": "Juárez Celman",
            "presidente roque saenz pena": "Presidente Roque Sáenz Peña",
            "union": "Unión",
            "marcos juarez": "Marcos Juárez",
            "bell ville": "Bell Ville",
            "terceros arriba": "Tercero Arriba",
            "villa maria": "Villa María",
            "general deheza": "General Deheza",
        }
        
        self.detail_cache = {}
        self.dumped_raw = False
        
    def get_all_properties(self, operation_ids: List[int] = None, page_size: int = 100) -> List[Dict]:
        if operation_ids is None:
            operation_ids = OPERATION_IDS
        all_properties = []
        page = 0
        max_props = MAX_PROPS  # 0 = sin tope

        while page < 500:  # tope duro de páginas por seguridad
            url = f"{self.base_url}/listings/findAll"
            params = {
                "page": page,
                "pageSize": page_size,
                "sort": "-createdAt",
                "in": f"operationId:{','.join(map(str, operation_ids))}",
                "in:typeId": "1,2,3,4,5,6,7,8",
                "withinarea": AREA_WITHINAREA  # polígono provincia de Córdoba
            }

            try:
                response = requests.get(url, params=params, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()

                properties = data.get("data", {}).get("data", [])
                if not properties:
                    if page == 0:
                        logger.error("Página 0 devolvió 0 propiedades — revisar el área/filtros")
                    break
                    
                if not self.dumped_raw and properties:
                    self.dump_raw_property(properties[0])
                    self.dumped_raw = True
                
                all_properties.extend(properties)
                logger.info(f"Página {page}: {len(properties)} propiedades (Total: {len(all_properties)})")

                if max_props and len(all_properties) >= max_props:
                    logger.info(f"Tope alcanzado ({max_props})")
                    all_properties = all_properties[:max_props]
                    break
                if len(properties) < page_size:
                    break
                    
                page += 1
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error en página {page}: {e}")
                break
        
        return all_properties
    
    def dump_raw_property(self, prop: Dict):
        logger.info("=" * 60)
        logger.info("DUMP DE PROPIEDAD CRUDA:")
        logger.info("=" * 60)
        logger.info(json.dumps(prop, indent=2, ensure_ascii=False))
        logger.info("=" * 60)
        
        with open("raw_property_dump.json", "w", encoding="utf-8") as f:
            json.dump(prop, f, indent=2, ensure_ascii=False)
        logger.info("Dump guardado en: raw_property_dump.json")
    
    def get_property_details(self, slug: str) -> Optional[Dict]:
        if not slug:
            return None
        
        if slug in self.detail_cache:
            return self.detail_cache[slug]

        url = f"{self.base_url}/listings/findBySlug/{slug}"
        for attempt in range(4):
            try:
                time.sleep(0.25)  # no saturar el endpoint de detalle
                response = requests.get(url, headers=self.headers, timeout=20)
                if response.status_code == 429 or response.status_code >= 500:
                    wait = 2 ** attempt
                    logger.warning(f"detalle {slug}: HTTP {response.status_code}, reintento en {wait}s")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                details = response.json().get("data", {}) or {}
                self.detail_cache[slug] = details
                return details
            except Exception as e:
                if attempt == 3:
                    logger.warning(f"detalle {slug} falló definitivo: {e}")
                    self.detail_cache[slug] = {}
                    return {}
                time.sleep(2 ** attempt)
        self.detail_cache[slug] = {}
        return {}
    
    def normalize_operation(self, raw_value: str) -> str:
        normalized = self.operation_map.get(raw_value.lower() if raw_value else "")
        if not normalized:
            return "venta"
        return normalized
    
    def normalize_status(self, raw_value: str) -> str:
        normalized = self.status_map.get(raw_value.lower() if raw_value else "")
        if not normalized:
            return "activa"
        return normalized
    
    def normalize_property_type(self, raw_value: str) -> str:
        normalized = self.property_type_map.get(raw_value.lower() if raw_value else "")
        if not normalized:
            return "otro"
        return normalized
    
    def title_case_es(self, text: str) -> str:
        if not text:
            return ""
        minusculas = {"de", "la", "el", "los", "las", "y", "o", "u", "del", "al"}
        words = text.split()
        result = []
        for i, word in enumerate(words):
            if i == 0 or word.lower() not in minusculas:
                result.append(word.capitalize())
            else:
                result.append(word.lower())
        return " ".join(result)
    
    def normalize_provincia(self, raw: str) -> str:
        if not raw:
            return ""
        clean = raw.strip().lower()
        return self.provincia_map.get(clean, self.title_case_es(clean))
    
    def normalize_partido(self, raw: str) -> str:
        if not raw:
            return ""
        clean = raw.strip().lower()
        return self.partido_map.get(clean, self.title_case_es(clean))
    
    def normalize_localidad(self, raw: str) -> str:
        if not raw:
            return ""
        return self.title_case_es(raw.strip())
    
    def normalize_barrio(self, raw: str) -> str:
        if not raw:
            return ""
        return self.title_case_es(raw.strip())
    
    def extract_geo_from_detail(self, details: Dict) -> Dict[str, str]:
        geo = details.get("geo")
        if not geo:
            return {}
        
        provincia = self.normalize_provincia(geo.get("state", ""))
        partido = self.normalize_partido(geo.get("countie", ""))
        localidad = self.normalize_localidad(geo.get("citie", ""))
        neighborhood = self.normalize_barrio(geo.get("neighborhood", ""))
        
        raw_private = geo.get("privatecommunitie")
        barrio_privado = self.normalize_barrio(raw_private) if raw_private else ""
        
        geo_label = geo.get("label", "")
        
        return {
            "provincia": provincia,
            "partido": partido,
            "localidad": localidad,
            "neighborhood": neighborhood,
            "barrio_privado": barrio_privado,
            "geo_label": geo_label
        }
    
    def parse_address_info(self, address_info: str) -> Dict[str, str]:
        if not address_info:
            return {}
        
        parts = [p.strip() for p in address_info.split(",")]
        
        provincia = self.normalize_provincia(parts[-1]) if len(parts) >= 1 else ""
        partido = self.normalize_partido(parts[-2]) if len(parts) >= 2 else ""
        localidad = self.normalize_localidad(parts[-3]) if len(parts) >= 3 else ""
        neighborhood = self.normalize_barrio(parts[0]) if len(parts) >= 4 else ""
        
        if len(parts) == 3:
            neighborhood = ""
            localidad = self.normalize_localidad(parts[0])
        
        return {
            "provincia": provincia,
            "partido": partido,
            "localidad": localidad,
            "neighborhood": neighborhood,
            "barrio_privado": "",
            "geo_label": address_info
        }
    
    def extract_primary_phone(self, phones: List[Dict]) -> str:
        if not phones:
            return ""
        primary = next((p for p in phones if p.get("primary")), None)
        if primary:
            return primary.get("value", "")
        return phones[0].get("value", "")
    
    def extract_primary_email(self, emails: List[Dict]) -> str:
        if not emails:
            return ""
        primary = next((e for e in emails if e.get("primary")), None)
        if primary:
            return primary.get("value", "")
        return emails[0].get("value", "")
    
    def normalize_broker_license(self, raw: str) -> str:
        if not raw:
            return ""
        return re.sub(r'\s+', ' ', raw).strip()
    
    def safe_int(self, value, default=0) -> int:
        """Convierte a int de forma segura"""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def safe_float(self, value) -> Optional[float]:
        """Convierte a float de forma segura, devuelve None si es 0 o inválido"""
        if value is None:
            return None
        try:
            f = float(value)
            return f if f > 0 else None
        except (ValueError, TypeError):
            return None
    
    def normalize_property(self, prop: Dict) -> Optional[Dict]:
        try:
            # === IDENTIFICACIÓN (del findAll) ===
            source_id = prop.get("internalId")
            if not source_id:
                logger.warning(f"Propiedad sin internalId (id={prop.get('id')}), descartando")
                return None
            
            slug = prop.get("slug")
            if not slug:
                logger.warning(f"Propiedad {source_id} sin slug, descartando")
                return None
            
            # === DETALLE COMPLETO (findBySlug) — si falla, seguimos con datos de lista ===
            details = self.get_property_details(slug) or {}
            detail_ok = bool(details.get("geo") or details.get("description"))

            # === CLASIFICACIÓN (del findAll, confiable) ===
            operation_raw = prop.get("operation", {}).get("value", "sale")
            operation = self.normalize_operation(operation_raw)
            
            type_raw = prop.get("type", {}).get("value", "")
            property_type = self.normalize_property_type(type_raw)
            
            status_raw = prop.get("listingStatus", {}).get("value", "active")
            status = self.normalize_status(status_raw)
            
            # === PRECIO (del findAll, confiable) ===
            price = prop.get("price")
            currency = prop.get("currency", {}).get("value", "USD")
            expenses = prop.get("expensesPrice")
            
            # === BLOQUE FÍSICO COMPLETO (SOLO del detalle) ===
            bedrooms = self.safe_int(details.get("bedrooms"), 0)
            bathrooms = self.safe_int(details.get("bathrooms"), 0)
            toilets = self.safe_int(details.get("toilets"), 0)
            total_rooms = self.safe_int(details.get("totalRooms"), 0)
            garages = self.safe_int(details.get("parkingSpaces"), 0)
            
            covered_m2 = self.safe_float(details.get("dimensionCovered"))
            total_m2 = self.safe_float(details.get("dimensionTotalBuilt"))
            land_m2 = self.safe_float(details.get("dimensionLand"))
            
            # Bonus: dimensiones adicionales
            dimension_uncovered = self.safe_float(details.get("dimensionUncovered"))
            dimension_semicovered = self.safe_float(details.get("dimensionSemicovered"))
            
            # === UBICACIÓN GPS (del findAll, confiable) ===
            location = prop.get("location", {})
            coordinates = location.get("coordinates", [])
            lat = coordinates[1] if len(coordinates) >= 2 else None
            lng = coordinates[0] if len(coordinates) >= 1 else None
            
            display_address = prop.get("displayAddress", "")
            
            # === GEO (del detalle, con fallback a addressInfo del findAll) ===
            geo_data = self.extract_geo_from_detail(details)
            if not geo_data.get("provincia"):
                address_info = prop.get("addressInfo", "")
                geo_data = self.parse_address_info(address_info)
            
            provincia = geo_data.get("provincia", "")
            partido = geo_data.get("partido", "")
            localidad = geo_data.get("localidad", "")
            neighborhood = geo_data.get("neighborhood", "")
            barrio_privado = geo_data.get("barrio_privado", "")
            geo_label = geo_data.get("geo_label", "")
            
            # === DESCRIPCIÓN Y FEATURES (del detalle) ===
            description = details.get("description", "") or ""
            
            features_raw = details.get("features", [])
            features = []
            if isinstance(features_raw, list):
                features = [f.get("value", "") for f in features_raw if f.get("value")]
            
            # === BONUS: flags del detalle ===
            apt_credit = bool(details.get("aptCredit", False))
            in_private_community = bool(details.get("inPrivateCommunity", False))
            pozo = bool(details.get("pozo", False))
            financing = bool(details.get("financing", False))
            year_built = self.safe_int(details.get("yearBuilt"), 0) or None
            
            # === AGENTE (del findAll) ===
            associate = prop.get("associate", {})
            agent_id = associate.get("id", "")
            if not agent_id:
                logger.warning(f"Propiedad {source_id} sin agent_id")
            
            agent_name = associate.get("name", "")
            
            agent_phones = associate.get("phones", [])
            agent_phone = self.extract_primary_phone(agent_phones)
            
            agent_emails = associate.get("emails", [])
            agent_email = self.extract_primary_email(agent_emails)
            
            # === OFICINA (objeto anidado del findAll) ===
            office_obj = associate.get("office", {})
            office_name = office_obj.get("name", "") if office_obj else ""
            office_id = office_obj.get("id", "") if office_obj else ""
            
            # === CORREDOR (del findAll) ===
            list_broker = prop.get("listBroker", [])
            broker = list_broker[0] if list_broker else {}
            broker_name = broker.get("name", "")
            broker_license = self.normalize_broker_license(broker.get("license", ""))
            
            # === FOTO (del findAll) ===
            photos = prop.get("photos", [])
            cover_photo = ""
            if photos:
                raw_photo = photos[0].get("rawValue", "")
                if raw_photo and not raw_photo.startswith("http"):
                    cover_photo = f"https://cdn.remax.com.ar/{raw_photo}"
                else:
                    cover_photo = raw_photo
            
            # === SOURCE URL ===
            source_url = f"https://www.remax.com.ar/propiedad/{source_id}"
            
            title = prop.get("title", "")
            
            normalized = {
                # Identificación
                "source": "remax",
                "source_id": source_id,
                "source_url": source_url,
                
                # Clasificación
                "operation": operation,
                "property_type": property_type,
                "raw_type": type_raw,
                "status": status,
                
                # Precio
                "price": price,
                "currency": currency,
                "expenses": expenses,
                
                # Bloque físico (TODO del detalle)
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "toilets": toilets,
                "total_rooms": total_rooms,
                "garages": garages,
                "covered_m2": covered_m2,
                "total_m2": total_m2,
                "land_m2": land_m2,
                "dimension_uncovered": dimension_uncovered,
                "dimension_semicovered": dimension_semicovered,
                
                # Ubicación
                "address": display_address,
                "neighborhood": neighborhood,
                "localidad": localidad,
                "partido": partido,
                "provincia": provincia,
                "barrio_privado": barrio_privado,
                "geo_label": geo_label,
                "lat": lat,
                "lng": lng,
                
                # Contenido
                "title": title,
                "description": description,
                "features": features,
                
                # Bonus flags
                "apt_credit": apt_credit,
                "in_private_community": in_private_community,
                "detail_ok": detail_ok,
                "pozo": pozo,
                "financing": financing,
                "year_built": year_built,
                
                # Agente y oficina
                "agent_id": agent_id,
                "agent_name": agent_name,
                "agent_phone": agent_phone,
                "agent_email": agent_email,
                "office_name": office_name,
                "office_id": office_id,
                "broker_name": broker_name,
                "broker_license": broker_license,
                
                # Foto y metadata
                "cover_photo": cover_photo,
                "scraped_at": self.scraped_at
            }
            
            return normalized
            
        except Exception as e:
            logger.error(f"Error normalizando propiedad {prop.get('id')}: {e}")
            return None
    
    def scrape_to_ndjson(self, output_file: str = "remax_cordoba_properties.ndjson"):
        logger.info("Iniciando scraping de RE/MAX Córdoba...")
        
        properties_raw = self.get_all_properties()
        logger.info(f"Total propiedades crudas obtenidas: {len(properties_raw)}")
        
        properties_normalized = []
        skipped_no_internal = 0
        skipped_no_slug = 0
        skipped_no_detail = 0
        skipped_fuera_prov = 0

        only_prov = ONLY_PROVINCIA.strip().lower()
        total_raw = len(properties_raw)

        for idx, prop in enumerate(properties_raw):
            if (idx + 1) % 200 == 0:
                logger.info(f"  normalizando {idx + 1}/{total_raw} (ok={len(properties_normalized)})...")

            source_id = prop.get("internalId")
            if not source_id:
                skipped_no_internal += 1
                continue

            slug = prop.get("slug")
            if not slug:
                skipped_no_slug += 1
                continue

            # Filtro de provincia ANTES de la llamada de detalle (ahorra requests):
            # addressInfo del objeto de lista termina en la provincia ("..., Córdoba").
            if only_prov:
                ai = (prop.get("addressInfo") or "").strip().lower()
                if ai and not ai.endswith(only_prov):
                    skipped_fuera_prov += 1
                    continue

            normalized = self.normalize_property(prop)
            if normalized:
                if not normalized.get("detail_ok"):
                    skipped_no_detail += 1  # se cargó igual, con datos de lista
                properties_normalized.append(normalized)
        
        logger.info(f"Total propiedades normalizadas: {len(properties_normalized)}")
        if skipped_no_internal > 0:
            logger.info(f"Descartadas sin internalId: {skipped_no_internal}")
        if skipped_no_slug > 0:
            logger.info(f"Descartadas sin slug: {skipped_no_slug}")
        if skipped_no_detail > 0:
            logger.info(f"Sin detalle (cargadas con datos de lista): {skipped_no_detail}")
        if skipped_fuera_prov > 0:
            logger.info(f"Descartadas fuera de provincia ({ONLY_PROVINCIA}): {skipped_fuera_prov}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for prop in properties_normalized:
                f.write(json.dumps(prop, ensure_ascii=False) + "\n")
        
        logger.info(f"Datos guardados en: {output_file}")
        
        json_file = output_file.replace('.ndjson', '.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(properties_normalized, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON array guardado en: {json_file}")
        
        # Reporte de tipos "otro"
        other_types = [p for p in properties_normalized if p.get("property_type") == "otro"]
        if other_types:
            logger.info(f"\n{'='*60}")
            logger.info(f"TIPOS 'OTRO' ENCONTRADOS ({len(other_types)}):")
            logger.info(f"{'='*60}")
            for prop in other_types:
                logger.info(f"  - source_id: {prop['source_id']}, raw_type: '{prop['raw_type']}', title: {prop['title'][:50]}")
        
        # Stats de campos físicos
        if properties_normalized:
            with_bedrooms = sum(1 for p in properties_normalized if p.get("bedrooms", 0) > 0)
            with_bathrooms = sum(1 for p in properties_normalized if p.get("bathrooms", 0) > 0)
            with_covered = sum(1 for p in properties_normalized if p.get("covered_m2") is not None)
            with_total = sum(1 for p in properties_normalized if p.get("total_m2") is not None)
            with_description = sum(1 for p in properties_normalized if p.get("description"))
            with_features = sum(1 for p in properties_normalized if len(p.get("features", [])) > 0)
            
            logger.info(f"\n{'='*60}")
            logger.info(f"STATS DE CAMPOS FÍSICOS:")
            logger.info(f"{'='*60}")
            logger.info(f"  Con bedrooms: {with_bedrooms}/{len(properties_normalized)}")
            logger.info(f"  Con bathrooms: {with_bathrooms}/{len(properties_normalized)}")
            logger.info(f"  Con covered_m2: {with_covered}/{len(properties_normalized)}")
            logger.info(f"  Con total_m2: {with_total}/{len(properties_normalized)}")
            logger.info(f"  Con description: {with_description}/{len(properties_normalized)}")
            logger.info(f"  Con features: {with_features}/{len(properties_normalized)}")
        
        return properties_normalized


if __name__ == "__main__":
    scraper = RemaxCordobaScraper()
    properties = scraper.scrape_to_ndjson("remax_cordoba_properties.ndjson")

    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETADO — {len(properties)} propiedades")
    print(f"{'='*60}")

    if not properties:
        logger.error("0 propiedades — no se envía a ingest")
        sys.exit(1)

    post_to_ingest(properties)