#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any

import cloudscraper

# ===== Настройки =====
DEAL_TYPE = "rent"  # "rent" — аренда, "sale" — продажа
REGION = "moscow"
ROOMS: list[int | str] | None = None  # например [1, 2, "studio"] или None для всех
REQUEST_DELAY_SEC = 1.0

DATASET_NAME = f"{REGION}_{DEAL_TYPE}_all"
JSON_PATH = Path("output") / f"{DATASET_NAME}.json"
CSV_PATH = Path("output") / f"{DATASET_NAME}.csv"
CHECKPOINT_PATH = Path("output") / f"{DATASET_NAME}_checkpoint.json"
LOG_FILE = Path("output/logs") / "01_collect_base.log"

# ===== Константы CIAN =====
API_URL = "https://api.cian.ru/search-offers/v2/search-offers-desktop/"
MAX_CIAN_PAGE = 54
OFFERS_PER_PAGE = 28
MAX_OFFERS_PER_SLICE = 50 * OFFERS_PER_PAGE
MIN_PRICE_STEP = 1_000
REQUEST_RETRIES = 3

REGIONS = {
    "moscow": 1,
    "москва": 1,
    "saint_petersburg": 2,
    "санкт-петербург": 2,
    "spb": 2,
}
ROOM_CODES = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, "studio": 9}
ROOM_GROUPS: tuple[tuple[int | str, ...], ...] = (
    (1,),
    (2,),
    (3,),
    (4,),
    (5,),
    ("studio",),
)
DEFAULT_MAX_PRICE = {"rent": 2_000_000, "sale": 1_000_000_000}

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


def sleep_with_stop(seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if stop_requested:
            return True
        time.sleep(min(0.2, deadline - time.monotonic()))
    return stop_requested


def make_session() -> cloudscraper.CloudScraper:
    session = cloudscraper.create_scraper()
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://www.cian.ru",
            "Referer": "https://www.cian.ru/",
        }
    )
    return session


def resolve_region_id(region: str | int) -> int:
    if isinstance(region, int):
        return region
    key = region.strip().lower().replace(" ", "_")
    if key not in REGIONS:
        raise ValueError(
            f"Неизвестный регион: {region!r}. Можно указать id региона CIAN."
        )
    return REGIONS[key]


def normalize_rooms(rooms: list[int | str] | tuple[int | str, ...] | None) -> list[int]:
    if not rooms:
        return []
    result: list[int] = []
    for room in rooms:
        if room not in ROOM_CODES:
            raise ValueError(f"Неподдерживаемое значение ROOMS: {room!r}")
        code = ROOM_CODES[room]
        if code not in result:
            result.append(code)
    return result


def build_query(
    *,
    deal_type: str,
    region_id: int,
    rooms: list[int | str] | tuple[int | str, ...] | None,
    page: int,
    min_price: int | None,
    max_price: int | None,
) -> dict[str, Any]:
    offer_type = "flatrent" if deal_type == "rent" else "flatsale"
    query: dict[str, Any] = {
        "region": {"type": "terms", "value": [region_id]},
        "engine_version": {"type": "term", "value": 2},
        "_type": offer_type,
        "page": {"type": "term", "value": page},
    }
    if deal_type == "rent":
        query["for_day"] = {"type": "term", "value": "!1"}

    room_values = normalize_rooms(rooms)
    if room_values:
        query["room"] = {"type": "terms", "value": room_values}

    if min_price is not None:
        query["price"] = {"type": "range", "value": {"gte": min_price}}
    if max_price is not None:
        query.setdefault("price", {"type": "range", "value": {}})["value"][
            "lte"
        ] = max_price

    return {"jsonQuery": query}


def request_search(
    session: cloudscraper.CloudScraper, payload: dict[str, Any]
) -> dict[str, Any]:
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            response = session.post(API_URL, json=payload, timeout=30)
            response.raise_for_status()
            return response.json().get("data", {})
        except Exception as exc:
            if attempt == REQUEST_RETRIES:
                raise
            logger.warning(
                "Ошибка запроса CIAN (%s/%s): %s", attempt, REQUEST_RETRIES, exc
            )
            sleep_with_stop(3 * attempt)
    return {}


def parse_images(raw_offer: dict[str, Any]) -> list[dict[str, Any]]:
    images: list[dict[str, Any]] = []
    for photo in raw_offer.get("photos") or []:
        url = photo.get("fullUrl") or photo.get("url")
        if not url:
            continue
        images.append(
            {
                "image_id": photo.get("id"),
                "url": url,
                "is_layout": bool(photo.get("isLayout")),
                "local_path": None,
            }
        )
    return images


def parse_author(user: dict[str, Any]) -> tuple[str | None, str | None]:
    if not user:
        return None, None
    if user.get("isBuilder"):
        return user.get("agencyName") or user.get("companyName"), "developer"
    if user.get("isAgent"):
        return user.get("agencyName") or user.get("agentName"), "agent"
    if user.get("accountType") == "agency":
        return user.get("agencyName"), "agency"
    name = (
        user.get("agencyName")
        or user.get("agentName")
        or user.get("companyName")
        or user.get("cianUserId")
    )
    return name, user.get("accountType")


def parse_offer(raw: dict[str, Any]) -> dict[str, Any]:
    bargain = raw.get("bargainTerms") or {}
    building = raw.get("building") or {}
    geo = raw.get("geo") or {}
    user = raw.get("user") or {}
    jk = geo.get("jk") or raw.get("jk")

    metro_name = None
    metro_time = None
    undergrounds = geo.get("undergrounds") or []
    if undergrounds:
        metro = next(
            (item for item in undergrounds if item.get("isDefault")), undergrounds[0]
        )
        metro_name = metro.get("name")
        metro_time = metro.get("time")

    district = None
    districts = geo.get("districts") or []
    if districts:
        district = districts[0].get("title") or districts[0].get("name")

    author, author_type = parse_author(user)
    area_raw = raw.get("totalArea")

    offer = {
        "offer_id": int(raw.get("cianId") or raw.get("id") or 0),
        "deal_type": DEAL_TYPE,
        "url": raw.get("fullUrl") or "",
        "title": raw.get("title") or "",
        "price_rub": bargain.get("priceRur") or bargain.get("price"),
        "price_formatted": raw.get("formattedFullPrice"),
        "rooms": raw.get("roomsCount"),
        "area_sqm": float(area_raw) if area_raw else None,
        "floor": raw.get("floorNumber"),
        "floors_total": building.get("floorsCount"),
        "address": geo.get("userInput"),
        "district": district,
        "metro": metro_name,
        "metro_time_min": metro_time,
        "author": author,
        "author_type": author_type,
        "residential_complex": (jk or {}).get("name") if isinstance(jk, dict) else None,
        "added": raw.get("added"),
        "description": raw.get("description"),
        "images": parse_images(raw),
    }
    for field in DETAIL_FIELDS:
        offer[field] = (
            {}
            if field == "detail_features"
            else False if field == "details_fetched" else None
        )
    return offer


def search(
    session: cloudscraper.CloudScraper,
    *,
    rooms: list[int | str] | tuple[int | str, ...] | None,
    page: int,
    min_price: int | None = None,
    max_price: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    payload = build_query(
        deal_type=DEAL_TYPE,
        region_id=resolve_region_id(REGION),
        rooms=rooms,
        page=page,
        min_price=min_price,
        max_price=max_price,
    )
    data = request_search(session, payload)
    offers = [parse_offer(item) for item in data.get("offersSerialized", [])]
    offers = [offer for offer in offers if offer["offer_id"]]
    total = int(data.get("aggregatedCount") or data.get("offerCount") or 0)
    return offers, total


def count_offers(
    session: cloudscraper.CloudScraper,
    rooms: tuple[int | str, ...] | None,
    min_price: int | None,
    max_price: int | None,
) -> int:
    _, total = search(
        session, rooms=rooms, page=1, min_price=min_price, max_price=max_price
    )
    return total


def slice_key(slice_: dict[str, Any]) -> str:
    rooms = slice_.get("rooms")
    rooms_label = ",".join(str(room) for room in rooms) if rooms else "all"
    return f"rooms={rooms_label};min={slice_.get('min_price')};max={slice_.get('max_price')}"


def slice_label(slice_: dict[str, Any]) -> str:
    rooms = slice_.get("rooms")
    rooms_label = ",".join(str(room) for room in rooms) if rooms else "all"
    return f"rooms={rooms_label}, price={slice_.get('min_price') or 0}..{slice_.get('max_price') or 'inf'}"


def partition_price(
    session: cloudscraper.CloudScraper,
    rooms: tuple[int | str, ...] | None,
    min_price: int | None,
    max_price: int | None,
) -> list[dict[str, Any]]:
    if stop_requested:
        return []

    total = count_offers(session, rooms, min_price, max_price)
    if total == 0:
        return []
    if total <= MAX_OFFERS_PER_SLICE:
        return [
            {
                "rooms": list(rooms) if rooms else None,
                "min_price": min_price,
                "max_price": max_price,
            }
        ]

    resolved_min = min_price if min_price is not None else 0
    resolved_max = max_price if max_price is not None else DEFAULT_MAX_PRICE[DEAL_TYPE]
    if resolved_max - resolved_min <= MIN_PRICE_STEP:
        logger.warning(
            "Срез всё ещё большой (%s объявлений), но дальше не делится", total
        )
        return [
            {
                "rooms": list(rooms) if rooms else None,
                "min_price": min_price,
                "max_price": max_price,
            }
        ]

    mid = (resolved_min + resolved_max) // 2
    left = partition_price(session, rooms, min_price, mid)
    right = partition_price(session, rooms, mid + 1, max_price)
    return left + right


def build_slices(session: cloudscraper.CloudScraper) -> list[dict[str, Any]]:
    room_groups = [tuple(ROOMS)] if ROOMS else list(ROOM_GROUPS)
    slices: list[dict[str, Any]] = []
    for rooms in room_groups:
        if stop_requested:
            break
        logger.info("Планирую срезы для rooms=%s", rooms)
        slices.extend(partition_price(session, rooms, None, None))
    logger.info("Построено срезов: %s", len(slices))
    return slices


def load_json_offers() -> dict[int, dict[str, Any]]:
    if not JSON_PATH.exists():
        return {}
    raw = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    offers = {int(item["offer_id"]): item for item in raw if item.get("offer_id")}
    logger.info("Загружено существующих объявлений: %s", len(offers))
    return offers


def offer_for_export(offer: dict[str, Any]) -> dict[str, Any]:
    data = dict(offer)
    images = data.get("images") or []
    data["image_urls"] = [image.get("url") for image in images if image.get("url")]
    data["image_paths"] = [
        image.get("local_path") for image in images if image.get("local_path")
    ]
    data["images_count"] = len(images)
    return data


def save_json(offers_by_id: dict[int, dict[str, Any]]) -> None:
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = [offer_for_export(offer) for offer in offers_by_id.values()]
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


def save_csv(offers_by_id: dict[int, dict[str, Any]]) -> None:
    rows = [flatten_for_csv(offer_for_export(offer)) for offer in offers_by_id.values()]
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


def load_checkpoint() -> dict[str, Any]:
    if not CHECKPOINT_PATH.exists():
        return {
            "deal_type": DEAL_TYPE,
            "region": REGION,
            "rooms": ROOMS,
            "slices_plan": [],
            "slices_plan_complete": False,
            "completed_slice_keys": [],
            "current_slice_key": None,
            "current_slice_next_page": 1,
        }

    checkpoint = json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
    if checkpoint.get("deal_type") and checkpoint.get("deal_type") != DEAL_TYPE:
        raise ValueError("Checkpoint создан для другого DEAL_TYPE")
    if checkpoint.get("region") and str(checkpoint.get("region")) != str(REGION):
        raise ValueError("Checkpoint создан для другого REGION")
    if checkpoint.get("rooms") != ROOMS:
        raise ValueError("Checkpoint создан для другого ROOMS")
    logger.info("Продолжаю из checkpoint: %s", CHECKPOINT_PATH)
    return checkpoint


def save_checkpoint(checkpoint: dict[str, Any]) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.write_text(
        json.dumps(checkpoint, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def checkpoint_plan_complete(checkpoint: dict[str, Any]) -> bool:
    return bool(
        checkpoint.get("slices_plan_complete")
        or (checkpoint.get("settings") or {}).get("slices_plan_complete")
    )


def preserve_enriched_fields(
    new_offer: dict[str, Any], old_offer: dict[str, Any]
) -> None:
    old_images = old_offer.get("images") or []
    if old_images and not new_offer.get("images"):
        new_offer["images"] = old_images

    paths_by_url = {
        image.get("url"): image.get("local_path")
        for image in old_images
        if image.get("url") and image.get("local_path")
    }
    for image in new_offer.get("images") or []:
        if not image.get("local_path"):
            image["local_path"] = paths_by_url.get(image.get("url"))

    for field in DETAIL_FIELDS:
        if field in old_offer:
            value = old_offer[field]
            new_offer[field] = (
                dict(value)
                if field == "detail_features" and isinstance(value, dict)
                else value
            )


def merge_offers(
    offers_by_id: dict[int, dict[str, Any]], new_offers: list[dict[str, Any]]
) -> int:
    added = 0
    for offer in new_offers:
        offer_id = int(offer["offer_id"])
        old_offer = offers_by_id.get(offer_id)
        if old_offer:
            preserve_enriched_fields(offer, old_offer)
        else:
            added += 1
        offers_by_id[offer_id] = offer
    return added


def mark_slice_done(checkpoint: dict[str, Any], key: str) -> None:
    completed = checkpoint.setdefault("completed_slice_keys", [])
    if key not in completed:
        completed.append(key)
    checkpoint["current_slice_key"] = None
    checkpoint["current_slice_next_page"] = 1


def collect(
    session: cloudscraper.CloudScraper,
    checkpoint: dict[str, Any],
    offers_by_id: dict[int, dict[str, Any]],
) -> None:
    if checkpoint.get("slices_plan") and checkpoint_plan_complete(checkpoint):
        slices = checkpoint["slices_plan"]
        logger.info("Загружен готовый план: %s срезов", len(slices))
    else:
        slices = build_slices(session)
        if stop_requested:
            logger.warning(
                "Остановка во время построения плана. План будет создан заново при следующем запуске."
            )
            return
        checkpoint["slices_plan"] = slices
        checkpoint["slices_plan_complete"] = True
        save_checkpoint(checkpoint)

    completed = set(checkpoint.get("completed_slice_keys") or [])
    logger.info(
        "Сбор: всего срезов %s, осталось %s", len(slices), len(slices) - len(completed)
    )

    for slice_ in slices:
        key = slice_key(slice_)
        if key in completed:
            continue
        if stop_requested:
            break

        start_page = 1
        if checkpoint.get("current_slice_key") == key:
            start_page = int(checkpoint.get("current_slice_next_page") or 1)

        logger.info("Срез %s, с page=%s", slice_label(slice_), start_page)
        for page in range(start_page, MAX_CIAN_PAGE + 1):
            if stop_requested:
                checkpoint["current_slice_key"] = key
                checkpoint["current_slice_next_page"] = page
                save_checkpoint(checkpoint)
                return

            offers, total = search(
                session,
                rooms=tuple(slice_["rooms"]) if slice_.get("rooms") else None,
                page=page,
                min_price=slice_.get("min_price"),
                max_price=slice_.get("max_price"),
            )
            if not offers:
                logger.info("Срез закончен на page=%s", page)
                break

            added = merge_offers(offers_by_id, offers)
            checkpoint["current_slice_key"] = key
            checkpoint["current_slice_next_page"] = page + 1
            save_json(offers_by_id)
            save_checkpoint(checkpoint)
            logger.info(
                "page=%s | объявлений=%s | новых=%s | всего сохранено=%s | всего на CIAN в срезе=%s",
                page,
                len(offers),
                added,
                len(offers_by_id),
                total,
            )

            if page == MAX_CIAN_PAGE:
                logger.warning(
                    "Достигнут лимит CIAN в %s страниц для среза %s",
                    MAX_CIAN_PAGE,
                    slice_label(slice_),
                )
            if sleep_with_stop(REQUEST_DELAY_SEC):
                return

        mark_slice_done(checkpoint, key)
        save_checkpoint(checkpoint)


def main() -> None:
    setup_logging()
    install_signal_handlers()

    if DEAL_TYPE not in {"rent", "sale"}:
        raise ValueError('DEAL_TYPE должен быть "rent" или "sale"')

    checkpoint = load_checkpoint()
    offers_by_id = load_json_offers()
    session = make_session()

    try:
        collect(session, checkpoint, offers_by_id)
    finally:
        save_json(offers_by_id)
        save_csv(offers_by_id)
        save_checkpoint(checkpoint)

    status = "остановлено, прогресс сохранён" if stop_requested else "готово"
    logger.info("%s: собрано %s объявлений", status, len(offers_by_id))
    logger.info("JSON: %s", JSON_PATH.resolve())
    logger.info("CSV: %s", CSV_PATH.resolve())


if __name__ == "__main__":
    main()
