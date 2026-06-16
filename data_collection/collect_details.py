#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import logging
import re
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from curl_cffi import requests as curl_requests

# ===== Настройки =====
DEAL_TYPE = "rent"
REGION = "moscow"
DATASET_NAME = f"{REGION}_{DEAL_TYPE}_all"
JSON_PATH = Path("output") / f"{DATASET_NAME}.json"
CSV_PATH = Path("output") / f"{DATASET_NAME}.csv"
LOG_FILE = Path("output/logs") / "03_collect_details.log"

DETAIL_WORKERS = 8
CHUNK_SIZE = 500
RETRIES = 2
TIMEOUT_SEC = 15.0
RETRY_DELAY_SEC = 1.0

# ===== API дополнительных характеристик CIAN =====
HISTORY_API_URL = (
    "https://api.cian.ru/valuation-offer-history/v2/get-offer-from-history-web/"
)
HISTORY_API_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.cian.ru",
    "priority": "u=1, i",
    "sec-ch-ua": '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}

REPAIR_LABEL_TO_TYPE = {
    "косметический": "cosmetic",
    "дизайнерский": "design",
    "евроремонт": "euro",
    "без ремонта": "no",
    "требует ремонта": "need",
}

DETAIL_FIELDS = (
    "repair",
    "repair_type",
    "ceiling_height_m",
    "build_year",
    "house_material",
    "heating_type",
    "parking_type",
    "kitchen_area_sqm",
    "living_area_sqm",
    "deposit_rub",
    "prepay_months",
    "utilities_included",
    "loggias_count",
    "balconies_count",
    "combined_wcs_count",
    "separate_wcs_count",
    "pets_allowed",
    "children_allowed",
    "passenger_lifts_count",
    "detail_features",
    "details_fetched",
)

logger = logging.getLogger(__name__)
stop_requested = False
thread_local = threading.local()


def setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
        force=True,
    )


def on_signal(signum: int, _frame: object) -> None:
    global stop_requested
    if stop_requested:
        raise SystemExit(1)
    stop_requested = True
    logger.warning(
        "Получен сигнал %s, сохраняю прогресс...", signal.Signals(signum).name
    )


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)


def get_thread_session() -> curl_requests.Session:
    session = getattr(thread_local, "session", None)
    if session is None:
        session = curl_requests.Session(impersonate="chrome124")
        thread_local.session = session
    return session


def ensure_detail_fields(offer: dict[str, Any]) -> None:
    for field in DETAIL_FIELDS:
        if field not in offer:
            offer[field] = (
                {}
                if field == "detail_features"
                else False if field == "details_fetched" else None
            )
    if not isinstance(offer.get("detail_features"), dict):
        offer["detail_features"] = {}
    offer["details_fetched"] = bool(offer.get("details_fetched"))


def load_offers() -> list[dict[str, Any]]:
    if not JSON_PATH.exists():
        raise SystemExit(f"Нет файла {JSON_PATH}. Сначала запустите collect_base.py")
    raw = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"В {JSON_PATH} должен быть JSON-массив")
    for offer in raw:
        ensure_detail_fields(offer)
    logger.info("Загружено объявлений: %s", len(raw))
    return raw


def offer_for_export(offer: dict[str, Any]) -> dict[str, Any]:
    data = dict(offer)
    images = data.get("images") or []
    data["image_urls"] = [image.get("url") for image in images if image.get("url")]
    data["image_paths"] = [
        image.get("local_path") for image in images if image.get("local_path")
    ]
    data["images_count"] = len(images)
    return data


def save_json(offers: list[dict[str, Any]]) -> None:
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = [offer_for_export(offer) for offer in offers]
    JSON_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def flatten_for_csv(row: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in row.items():
        if key == "images":
            continue
        if isinstance(value, list):
            result[key] = "|".join(str(item) for item in value)
        elif isinstance(value, dict):
            result[key] = json.dumps(value, ensure_ascii=False)
        else:
            result[key] = value
    return result


def save_csv(offers: list[dict[str, Any]]) -> None:
    rows = [flatten_for_csv(offer_for_export(offer)) for offer in offers]
    if not rows:
        return
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with CSV_PATH.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def offer_page_url(offer: dict[str, Any]) -> str:
    if offer.get("url"):
        parsed = urlparse(str(offer["url"]))
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    return f"https://www.cian.ru/{offer.get('deal_type') or DEAL_TYPE}/flat/{offer['offer_id']}/"


def history_headers(offer: dict[str, Any]) -> dict[str, str]:
    headers = dict(HISTORY_API_HEADERS)
    headers["referer"] = offer_page_url(offer)
    return headers


def iter_dicts(value: object) -> Iterable[dict[str, Any]]:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                yield item


def stringify_feature_value(value: object) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    return json.dumps(value, ensure_ascii=False)


def set_or_prefix_feature(
    values: dict[str, str], section_label: str, label: str, value: object
) -> None:
    text = stringify_feature_value(value)
    if text is None:
        return
    key = label
    if key in values and values[key] != text:
        key = f"{section_label}.{label}"
    values[key] = text


def set_feature(values: dict[str, str], label: str, value: object) -> None:
    text = stringify_feature_value(value)
    if text is not None:
        values.setdefault(label, text)


def feature_by_label(values: dict[str, str], *labels: str) -> str | None:
    for label in labels:
        if label in values:
            return values[label]
        suffix = f".{label}"
        for key, value in values.items():
            if key.endswith(suffix):
                return value
    return None


def history_api_feature_values(raw_details: dict[str, Any]) -> dict[str, str]:
    values: dict[str, str] = {}
    for section_key, section_label in (
        ("features", "Характеристики"),
        ("dealConditions", "Условия сделки"),
    ):
        for item in iter_dicts(raw_details.get(section_key)):
            label = item.get("title") or item.get("label")
            if label:
                set_or_prefix_feature(
                    values, section_label, str(label), item.get("value")
                )
    return values


def normalize_text(value: object) -> str:
    return str(value).replace("\xa0", " ").replace("\u202f", " ").strip().lower()


def parse_optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    normalized = normalize_text(value).replace(" ", "").replace(",", ".")
    match = re.search(r"\d+(?:\.\d+)?", normalized)
    return float(match.group()) if match else None


def parse_optional_int(value: object) -> int | None:
    parsed = parse_optional_float(value)
    return int(parsed) if parsed is not None else None


def parse_money_or_zero(value: object) -> int | None:
    if value is None:
        return None
    normalized = normalize_text(value)
    if normalized in {"нет", "без", "0"} or normalized.startswith("нет "):
        return 0
    return parse_optional_int(value)


def parse_utilities_included(value: object) -> bool | None:
    if value is None:
        return None
    normalized = normalize_text(value)
    if "не включ" in normalized or "отдель" in normalized:
        return False
    if "включ" in normalized:
        return True
    return None


def parse_count_by_stems(value: object, stems: tuple[str, ...]) -> int | None:
    if value is None:
        return None
    normalized = normalize_text(value).replace(",", " ")
    total = 0
    found = False
    for match in re.finditer(r"(\d+)\s*([а-яёa-z-]+)", normalized):
        word = match.group(2)
        if any(word.startswith(stem) for stem in stems):
            total += int(match.group(1))
            found = True
    if found:
        return total
    if "нет" not in normalized and any(stem in normalized for stem in stems):
        return 1
    return None


def parse_living_allowed(value: object, entity: str) -> bool | None:
    if value is None:
        return None
    normalized = normalize_text(value).replace("c ", "с ")

    if entity == "children":
        if not any(token in normalized for token in ("дет", "ребен", "ребён")):
            return None
        if any(
            token in normalized
            for token in (
                "без детей",
                "нельзя с детьми",
                "с детьми нельзя",
                "без ребен",
                "без ребён",
            )
        ):
            return False
        if "можно" in normalized or "разреш" in normalized:
            return True
        return None

    if not any(token in normalized for token in ("живот", "питом")):
        return None
    if any(
        token in normalized
        for token in ("без живот", "без питом", "нельзя с живот", "с животными нельзя")
    ):
        return False
    if "можно" in normalized or "разреш" in normalized:
        return True
    return None


def apply_history_details(offer: dict[str, Any], raw_details: dict[str, Any]) -> bool:
    history_features = history_api_feature_values(raw_details)
    if not history_features:
        return False

    detail_features = dict(offer.get("detail_features") or {})
    detail_features.update(history_features)

    repair_label = feature_by_label(detail_features, "Ремонт")
    ceiling_label = feature_by_label(detail_features, "Высота потолков")
    build_year_label = feature_by_label(detail_features, "Год постройки")
    house_material = feature_by_label(detail_features, "Тип дома")
    heating_type = feature_by_label(detail_features, "Отопление")
    parking_type = feature_by_label(detail_features, "Парковка")
    kitchen_label = feature_by_label(detail_features, "Площадь кухни", "Кухня")
    living_label = feature_by_label(detail_features, "Жилая площадь")
    deposit_label = feature_by_label(detail_features, "Залог", "Залога")
    prepay_label = feature_by_label(detail_features, "Предоплата")
    utilities_label = feature_by_label(detail_features, "Оплата ЖКХ")
    living_conditions = feature_by_label(detail_features, "Условия проживания")

    outdoor_text = " ".join(
        value
        for value in (
            feature_by_label(detail_features, "Балкон"),
            feature_by_label(detail_features, "Балконы"),
            feature_by_label(detail_features, "Лоджия"),
            feature_by_label(detail_features, "Лоджии"),
            feature_by_label(detail_features, "Балкон/лоджия"),
        )
        if value
    )
    wc_text = feature_by_label(detail_features, "Санузел", "Санузлы")
    lift_text = feature_by_label(detail_features, "Лифт", "Лифты")

    set_feature(detail_features, "Площадь кухни", kitchen_label)
    set_feature(detail_features, "Высота потолков", ceiling_label)

    if repair_label:
        offer["repair"] = repair_label
        offer["repair_type"] = REPAIR_LABEL_TO_TYPE.get(normalize_text(repair_label))
    if ceiling_label:
        offer["ceiling_height_m"] = parse_optional_float(ceiling_label)
    if build_year_label:
        offer["build_year"] = parse_optional_int(build_year_label)
    if house_material:
        offer["house_material"] = house_material
    if heating_type:
        offer["heating_type"] = heating_type
    if parking_type:
        offer["parking_type"] = parking_type
    if kitchen_label:
        offer["kitchen_area_sqm"] = parse_optional_float(kitchen_label)
    if living_label:
        offer["living_area_sqm"] = parse_optional_float(living_label)
    if deposit_label:
        offer["deposit_rub"] = parse_money_or_zero(deposit_label)
    if prepay_label:
        offer["prepay_months"] = parse_optional_int(prepay_label)

    utilities_included = parse_utilities_included(utilities_label)
    if utilities_included is not None:
        offer["utilities_included"] = utilities_included

    balconies_count = parse_count_by_stems(outdoor_text, ("балкон",))
    if balconies_count is not None:
        offer["balconies_count"] = balconies_count
    loggias_count = parse_count_by_stems(outdoor_text, ("лоджи",))
    if loggias_count is not None:
        offer["loggias_count"] = loggias_count

    if wc_text:
        combined_count = parse_count_by_stems(wc_text, ("совмещ",))
        separate_count = parse_count_by_stems(wc_text, ("раздель",))
        if combined_count is not None:
            offer["combined_wcs_count"] = combined_count
        if separate_count is not None:
            offer["separate_wcs_count"] = separate_count

    if lift_text:
        passenger_count = parse_count_by_stems(lift_text, ("пассажир",))
        if passenger_count is not None:
            offer["passenger_lifts_count"] = passenger_count

    children_allowed = parse_living_allowed(living_conditions, "children")
    if children_allowed is not None:
        offer["children_allowed"] = children_allowed
    pets_allowed = parse_living_allowed(living_conditions, "pets")
    if pets_allowed is not None:
        offer["pets_allowed"] = pets_allowed

    offer["detail_features"] = detail_features
    offer["details_fetched"] = True
    return True


def fetch_details(offer: dict[str, Any]) -> bool:
    url = f"{HISTORY_API_URL}?cianId={offer['offer_id']}"
    response = get_thread_session().get(
        url, headers=history_headers(offer), timeout=TIMEOUT_SEC
    )
    if response.status_code == 400:
        return False
    response.raise_for_status()
    raw_details = response.json()
    if not isinstance(raw_details, dict):
        return False
    if not raw_details.get("features") and not raw_details.get("dealConditions"):
        return False
    return apply_history_details(offer, raw_details)


def fetch_details_safely(offer: dict[str, Any]) -> bool:
    last_error: Exception | None = None
    for attempt in range(RETRIES + 1):
        if stop_requested:
            return False
        try:
            return fetch_details(offer)
        except Exception as exc:
            last_error = exc
            if attempt < RETRIES:
                time.sleep(RETRY_DELAY_SEC * (attempt + 1))
    logger.warning(
        "Не удалось собрать характеристики offer=%s: %s",
        offer.get("offer_id"),
        last_error,
    )
    return False


def enrich_chunk(chunk: list[dict[str, Any]]) -> dict[str, int]:
    stats = {"fetched": 0, "failed": 0}
    with ThreadPoolExecutor(max_workers=max(1, DETAIL_WORKERS)) as executor:
        futures = [executor.submit(fetch_details_safely, offer) for offer in chunk]
        for number, future in enumerate(as_completed(futures), start=1):
            if stop_requested:
                for item in futures:
                    item.cancel()
                break
            try:
                ok = future.result()
            except Exception as exc:
                logger.warning("Ошибка worker: %s", exc)
                ok = False
            if ok:
                stats["fetched"] += 1
            else:
                stats["failed"] += 1
            if number == 1 or number % 100 == 0 or number == len(chunk):
                logger.info(
                    "Прогресс пачки: %s/%s | собрано=%s ошибок=%s",
                    number,
                    len(chunk),
                    stats["fetched"],
                    stats["failed"],
                )
    return stats


def pending_offers(offers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [offer for offer in offers if not offer.get("details_fetched")]


def main() -> None:
    setup_logging()
    install_signal_handlers()

    offers = load_offers()
    pending = pending_offers(offers)
    logger.info("К обработке: %s из %s объявлений", len(pending), len(offers))

    if not pending:
        save_csv(offers)
        logger.info("Дополнительные характеристики уже собраны")
        return

    total_fetched = 0
    total_failed = 0
    try:
        for start in range(0, len(pending), CHUNK_SIZE):
            if stop_requested:
                break
            chunk = pending[start : start + CHUNK_SIZE]
            started_at = time.monotonic()
            stats = enrich_chunk(chunk)
            total_fetched += stats["fetched"]
            total_failed += stats["failed"]
            save_json(offers)
            logger.info(
                "Пачка %s-%s/%s готова за %.1f сек | собрано=%s ошибок=%s",
                start + 1,
                start + len(chunk),
                len(pending),
                time.monotonic() - started_at,
                stats["fetched"],
                stats["failed"],
            )
    finally:
        save_json(offers)
        save_csv(offers)

    left = len(pending_offers(offers))
    status = "остановлено, прогресс сохранён" if stop_requested else "готово"
    logger.info(
        "%s: за запуск собрано=%s ошибок=%s | всего с характеристиками=%s/%s | осталось=%s",
        status,
        total_fetched,
        total_failed,
        len(offers) - left,
        len(offers),
        left,
    )
    logger.info("JSON: %s", JSON_PATH.resolve())
    logger.info("CSV: %s", CSV_PATH.resolve())


if __name__ == "__main__":
    main()
