import time
import random
import unicodedata
import os
import sys
import datetime
import logging
import pandas as pd
import cloudscraper

from typing import List, Optional, Dict, Any
from google.cloud import storage

# ===================== Configuração de Logs =====================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("zap_job_scraper")

# ===================== Constantes ===============================
ORIGIN_PAGE = "https://www.zapimoveis.com.br/"
API_URL = "https://glue-api.zapimoveis.com.br/v4/listings"
DEVICE_ID = "c5a40c3c-d033-4a5d-b1e2-79b59e4fb68d"
PORTAL = "ZAP"
CATEGORY_PAGE = "RESULT"
LISTING_TYPE = "USED"

# --- Constantes de Comportamento ---

SIZE = 30
FROM_MAX = 300
PRICE_MIN_START = 1000
PRICE_STEP = 49990
REQUESTS_TIMEOUT = 30
BASE_SLEEP_SECONDS = 0.9
RANDOM_JITTER_MAX = 0.6
RETRIES = 5

USE_BROWSER_COOKIES = False

INCLUDE_FIELDS = (
    "expansion(search(result(listings(listing("
    "expansionType,contractType,listingsCount,propertyDevelopers,sourceId,displayAddressType,amenities,usableAreas,"
    "constructionStatus,listingType,description,title,stamps,createdAt,floors,unitTypes,nonActivationReason,providerId,"
    "propertyType,unitSubTypes,unitsOnTheFloor,legacyId,id,portal,unitFloor,parkingSpaces,updatedAt,address,suites,"
    "publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,advertiserContact,whatsappNumber,bedrooms,"
    "acceptExchange,pricingInfos,showPrice,resale,buildings,capacityLimit,status,priceSuggestion,condominiumName,modality,"
    "enhancedDevelopment),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,legacyZapId,createdDate,tier,"
    "trustScore,totalCountByFilter,totalCountByAdvertiser),medias,accountLink,link,children(id,usableAreas,totalAreas,"
    "bedrooms,bathrooms,parkingSpaces,pricingInfos))),totalCount)),fullUriFragments,nearby(search(result(listings(listing("
    "expansionType,contractType,listingsCount,propertyDevelopers,sourceId,displayAddressType,amenities,usableAreas,"
    "constructionStatus,listingType,description,title,stamps,createdAt,floors,unitTypes,nonActivationReason,providerId,"
    "propertyType,unitSubTypes,unitsOnTheFloor,legacyId,id,portal,unitFloor,parkingSpaces,updatedAt,address,suites,"
    "publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,advertiserContact,whatsappNumber,bedrooms,"
    "acceptExchange,pricingInfos,showPrice,resale,buildings,capacityLimit,status,priceSuggestion,condominiumName,modality,"
    "enhancedDevelopment),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,legacyZapId,createdDate,tier,"
    "trustScore,totalCountByFilter,totalCountByAdvertiser),medias,accountLink,link,children(id,usableAreas,totalAreas,"
    "bedrooms,bathrooms,parkingSpaces,pricingInfos))),totalCount)),page,search(result(listings(listing("
    "expansionType,contractType,listingsCount,propertyDevelopers,sourceId,displayAddressType,amenities,usableAreas,"
    "constructionStatus,listingType,description,title,stamps,createdAt,floors,unitTypes,nonActivationReason,providerId,"
    "propertyType,unitSubTypes,unitsOnTheFloor,legacyId,id,portal,unitFloor,parkingSpaces,updatedAt,address,suites,"
    "publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,advertiserContact,whatsappNumber,bedrooms,"
    "acceptExchange,pricingInfos,showPrice,resale,buildings,capacityLimit,status,priceSuggestion,condominiumName,modality,"
    "enhancedDevelopment),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,legacyZapId,createdDate,tier,"
    "trustScore,totalCountByFilter,totalCountByAdvertiser),medias,accountLink,link,children(id,usableAreas,totalAreas,"
    "bedrooms,bathrooms,parkingSpaces,pricingInfos))),totalCount),topoFixo(search(result(listings(listing("
    "expansionType,contractType,listingsCount,propertyDevelopers,sourceId,displayAddressType,amenities,usableAreas,"
    "constructionStatus,listingType,description,title,stamps,createdAt,floors,unitTypes,nonActivationReason,providerId,"
    "propertyType,unitSubTypes,unitsOnTheFloor,legacyId,id,portal,unitFloor,parkingSpaces,updatedAt,address,suites,"
    "publicationType,externalId,bathrooms,usageTypes,totalAreas,advertiserId,advertiserContact,whatsappNumber,bedrooms,"
    "acceptExchange,pricingInfos,showPrice,resale,buildings,capacityLimit,status,priceSuggestion,condominiumName,modality,"
    "enhancedDevelopment),account(id,name,logoUrl,licenseNumber,showAddress,legacyVivarealId,legacyZapId,createdDate,tier,"
    "trustScore,totalCountByFilter,totalCountByAdvertiser),medias,accountLink,link,children(id,usableAreas,totalAreas,"
    "bedrooms,bathrooms,parkingSpaces,pricingInfos))),totalCount))"
)

# ===================== Helpers =====================

def _ascii_no_accents(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")

def build_address_location_id(state: str, city: str) -> str:
    st = _ascii_no_accents(state)
    ct = _ascii_no_accents(city)
    return f"BR>{st}>NULL>{ct}"

UA_EDGE_141 = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
)

COMMON_HEADERS = {
    "User-Agent": UA_EDGE_141,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8,en-US;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Origin": "https://www.zapimoveis.com.br",
    "Referer": "https://www.zapimoveis.com.br/",
    "x-deviceid": DEVICE_ID,
    "x-domain": ".zapimoveis.com.br",
}

def make_scraper():
    s = cloudscraper.create_scraper()
    s.headers.update(COMMON_HEADERS)
    return s

def bootstrap_cookies() -> Dict[str, str]:
    s = make_scraper()
    try:
        r = s.get(ORIGIN_PAGE, timeout=REQUESTS_TIMEOUT)
        logger.info(f"Bootstrap origem: {r.status_code}")
    except Exception as e:
        logger.warning(f"Falha ao abrir origem: {e}")
    cookies = s.cookies.get_dict()
    keys = ", ".join(cookies.keys()) if cookies else "(nenhum)"
    logger.info(f"Cookies coletados via request inicial: {keys}")
    return cookies

def polite_sleep():
    time.sleep(BASE_SLEEP_SECONDS + random.uniform(0, RANDOM_JITTER_MAX))

def looks_like_html(text: str) -> bool:
    if not text: return False
    t = text.lstrip()
    return t.startswith("<") or t.lower().startswith("<!doctype")

def extract_listings(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    exp = (((payload or {}).get("expansion") or {}).get("search") or {}).get("result") or {}
    if isinstance(exp, dict) and "listings" in exp:
        return exp.get("listings") or []
    srch = ((payload or {}).get("search") or {}).get("result") or {}
    return srch.get("listings") or []

# ===================== Core: chamada da API =====================

def call_api(scraper, params: Dict[str, str], tries=RETRIES):
    last = None
    for i in range(1, tries + 1):
        try:
            r = scraper.get(API_URL, params=params, timeout=REQUESTS_TIMEOUT)
            ct = (r.headers.get("Content-Type") or "").lower()

            if r.status_code == 200 and "application/json" in ct:
                if looks_like_html(r.text):
                    raise ValueError("Corpo HTML com status 200")
                return r

            # Lógica de Backoff para erros 429 ou 5xx
            if r.status_code == 429 or 500 <= r.status_code < 600:
                wait = 1.2 * (2 ** (i - 1)) + random.uniform(0, 0.8)
                logger.warning(f"{r.status_code} na API (tentativa {i}/{tries}). Backoff {wait:.1f}s…")
                time.sleep(wait)
                last = r
                continue

            # Lógica para bloqueios
            if r.status_code in (401, 403) or ("text/html" in ct) or looks_like_html(r.text):
                logger.warning(f"Bloqueio/HTML (status={r.status_code}, ct={ct}). Tentativa {i}/{tries}.")
                time.sleep(0.8 + random.uniform(0, 0.6))
                last = r
                continue
            last = r
        except Exception as e:
            logger.warning(f"Exceção na chamada (tentativa {i}/{tries}): {e}")
            time.sleep(1.0 + random.uniform(0, 0.6))
    return last

# ===================== Upload para GCS =====================

def upload_df_to_gcs(df: pd.DataFrame, bucket_name: str, destination_blob_name: str, format: str = 'parquet'):
    temp_filename = os.path.join("/tmp", destination_blob_name.split('/')[-1])
    try:
        # Garante que o diretório pai exista (localmente, se necessário, mas no GCS é blob)
        if format == 'parquet':
            df.to_parquet(temp_filename, index=False, engine='pyarrow')
        elif format == 'csv':
            df.to_csv(temp_filename, index=False, encoding='utf-8')

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(temp_filename)
        logger.info(f"Arquivo salvo no GCS: gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        logger.error(f"Falha ao fazer upload para o GCS: {e}")
        raise
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

# ===================== Pipeline Lógico =====================

def run_pipeline(city: str, state: str, business_type: str, price_max: int) -> (Optional[pd.DataFrame], Dict[str, Any]):
    start_time = time.time()
    start_time_utc = datetime.datetime.utcnow()
    logger.info(f"Iniciando pipeline para {city}/{state} (Business: {business_type}, PriceMax: {price_max})")

    # 1) Cookies (Apenas Bootstrap simples em ambiente headless)
    cookies = bootstrap_cookies()
    scraper_api = make_scraper()
    if cookies:
        scraper_api.cookies.update(cookies)

    # 2) Params
    address_loc_id = build_address_location_id(state, city)
    base_params = {
        "user": DEVICE_ID,
        "portal": PORTAL,
        "categoryPage": CATEGORY_PAGE,
        "business": business_type,
        "listingType": LISTING_TYPE,
        "__zt": "mtc:deduplication2023",
        "addressCity": city,
        "addressLocationId": address_loc_id,
        "addressState": state,
        "addressType": "city",
        "size": str(SIZE),
        "images": "webp",
        "includeFields": INCLUDE_FIELDS,
    }

    rows = []
    status = "SUCCESS"

    # 3) Varredura
    try:
        for pmin in range(PRICE_MIN_START, price_max, PRICE_STEP):
            pmax = min(pmin + PRICE_STEP, price_max)
            logger.info(f"🔎 Faixa R$ {pmin} .. R$ {pmax}")

            for from_v in range(0, FROM_MAX, SIZE):
                page = (from_v // SIZE) + 1
                params = dict(base_params)
                params.update({
                    "page": str(page),
                    "from": str(from_v),
                    "priceMin": str(pmin),
                    "priceMax": str(pmax),
                })

                r = call_api(scraper_api, params)

                if r is None: break
                ct = (r.headers.get("Content-Type") or "").lower()
                if r.status_code == 404: break
                if r.status_code != 200 or "application/json" not in ct: break

                try:
                    data = r.json()
                except Exception:
                    continue

                listings = extract_listings(data)
                if not listings: break

                for it in listings:
                    lin = it.get("listing") or {}
                    lin["account"] = it.get("account")
                    lin["medias"] = it.get("medias")
                    lin["accountLink"] = it.get("accountLink")
                    lin["link"] = it.get("link")
                    rows.append(lin)

                logger.info(f"✔️ page={page} from={from_v} registros={len(listings)}")
                polite_sleep()
                if len(listings) < SIZE: break  # Fim da paginação

            time.sleep(random.uniform(1.2, 2.5))

    except Exception as e:
        logger.error(f"Erro fatal durante scraping: {e}")
        status = f"FAILURE: {str(e)}"

    # 4) Consolidação
    df = None
    total_records = 0
    size_in_bytes = 0

    if rows:
        try:
            df = pd.json_normalize(rows, sep=".")
            total_records = len(df)
            size_in_bytes = int(df.memory_usage(deep=True).sum())
            logger.info(f"✅ Coleta finalizada | shape={df.shape} | bytes={size_in_bytes}")
        except Exception as e:
            logger.error(f"Erro ao normalizar dados: {e}")
            status = f"FAILURE_NORMALIZE: {str(e)}"
    else:
        if status == "SUCCESS":
            status = "NO_DATA"

    end_time_utc = datetime.datetime.utcnow()

    metadata = {
        "execution_start_utc": start_time_utc.isoformat(),
        "execution_end_utc": end_time_utc.isoformat(),
        "total_duration_seconds": time.time() - start_time,
        "status": status,
        "city": city,
        "state": state,
        "business_type": business_type,
        "total_records": total_records,
        "data_size_bytes": size_in_bytes,
        "parameters": {
            "price_min": PRICE_MIN_START,
            "price_max": price_max,
            "price_step": PRICE_STEP
        }
    }

    return df, metadata


# ===================== Main (Cloud Run Job Entrypoint) =====================

GCS_BUCKET_NAME = "pdm-especulacoes-imobiliario"


def main():
    """
    Função principal executada pelo Container.
    Lê parâmetros de Variáveis de Ambiente (injetadas pelo Cloud Scheduler/Manual Job).
    """
    logger.info("--- Iniciando Job Cloud Run ---")

    # 1. Leitura de Parâmetros
    # (Recomendado usar Variáveis de Ambiente para os parâmetros de execução)
    city = os.environ.get("TARGET_CITY")
    state = os.environ.get("TARGET_STATE")
    business_type = os.environ.get("BUSINESS_TYPE", "SALE")
    price_max_env = os.environ.get("PRICE_MAX", "2000000")

    if not city or not state:
        # Se for rodar localmente ou via CLI, use sys.argv (opcional, dependendo da sua estratégia)
        if len(sys.argv) > 2:
            city = sys.argv[1]
            state = sys.argv[2]
            logger.warning("Usando city/state do CLI (sys.argv) em vez de variáveis de ambiente.")
        else:
            logger.error("CRITICAL: TARGET_CITY e TARGET_STATE são obrigatórias (via ENV ou CLI).")
            sys.exit(1)

    try:
        price_max = int(price_max_env)
    except ValueError:
        logger.error(f"CRITICAL: PRICE_MAX inválido: {price_max_env}")
        sys.exit(1)

    # 2. Execução
    try:
        (data_df, exec_metadata) = run_pipeline(
            city=city,
            state=state,
            business_type=business_type,
            price_max=price_max
        )

        # Prepara o caminho no GCS (Ex: SALE/Goiás/Goiânia/2025-11-20T18-20-00.000000)
        run_timestamp = exec_metadata['execution_start_utc'].replace(":", "-").replace(".", "-")
        base_path = f"{business_type}/{state}/{city}/{run_timestamp}"

        # 3. Uploads
        logger.info(f"Iniciando upload para o bucket: {GCS_BUCKET_NAME}")

        # Metadados: Salva no caminho 'metadata/...'
        metadata_df = pd.DataFrame([exec_metadata])
        upload_df_to_gcs(
            df=metadata_df,
            bucket_name=GCS_BUCKET_NAME,  # <--- CORREÇÃO AQUI
            destination_blob_name=f"metadata/{base_path}_metadata.parquet"
        )

        # Dados: Salva no caminho 'data/...'
        if data_df is not None and not data_df.empty:
            upload_df_to_gcs(
                df=data_df,
                bucket_name=GCS_BUCKET_NAME,
                destination_blob_name=f"data/{base_path}_data.parquet"
            )
        else:
            logger.warning(f"Nenhum dado para salvar em {city}. Apenas metadados foram salvos.")

        logger.info("--- Job Finalizado com Sucesso ---")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Erro não tratado na execução principal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()