#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import logging
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import cloudscraper

# ===== Настройки =====
DEAL_TYPE = "rent"
REGION = "moscow"
DATASET_NAME = f"{REGION}_{DEAL_TYPE}_all"
JSON_PATH = Path("output") / f"{DATASET_NAME}.json"
CSV_PATH = Path("output") / f"{DATASET_NAME}.csv"
IMAGES_DIR = Path("output/images") / f"{REGION}_{DEAL_TYPE}"
LOG_FILE = Path("output/logs") / "02_download_images.log"

MAX_IMAGES_PER_OFFER: int | None = None  # например 10 или None для всех
INCLUDE_LAYOUTS = True
IMAGE_WORKERS = 4
IMAGE_DELAY_SEC = 0.3  # используется при IMAGE_WORKERS = 1

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


def make_session() -> cloudscraper.CloudScraper:
    session = cloudscraper.create_scraper()
    session.headers.update(
        {
            "Accept": "image/*,*/*",
            "Origin": "https://www.cian.ru",
            "Referer": "https://www.cian.ru/",
        }
    )
    return session


def get_thread_session() -> cloudscraper.CloudScraper:
    session = getattr(thread_local, "session", None)
    if session is None:
        session = make_session()
        thread_local.session = session
    return session


def load_offers() -> list[dict[str, Any]]:
    if not JSON_PATH.exists():
        raise SystemExit(f"Нет файла {JSON_PATH}. Сначала запустите collect_base.py")
    raw = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"В {JSON_PATH} должен быть JSON-массив")
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


def target_for_image(offer_id: int, image: dict[str, Any], index: int) -> Path:
    suffix = "_layout" if image.get("is_layout") else ""
    extension = Path(urlparse(image["url"]).path).suffix or ".jpg"
    return IMAGES_DIR / str(offer_id) / f"{index:02d}{suffix}{extension}"


def build_tasks(
    offers: list[dict[str, Any]], stats: dict[str, int]
) -> list[tuple[int, dict[str, Any], Path]]:
    tasks: list[tuple[int, dict[str, Any], Path]] = []
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    for offer in offers:
        offer_id = int(offer.get("offer_id") or 0)
        images = [image for image in offer.get("images") or [] if image.get("url")]
        if not INCLUDE_LAYOUTS:
            images = [image for image in images if not image.get("is_layout")]
        if MAX_IMAGES_PER_OFFER is not None:
            images = images[:MAX_IMAGES_PER_OFFER]

        for index, image in enumerate(images, start=1):
            old_path = image.get("local_path")
            if old_path and Path(old_path).exists():
                stats["skipped"] += 1
                continue

            target = target_for_image(offer_id, image, index)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                image["local_path"] = str(target)
                stats["skipped"] += 1
                continue
            tasks.append((offer_id, image, target))

    return tasks


def download_one(task: tuple[int, dict[str, Any], Path]) -> str:
    offer_id, image, target = task
    if target.exists():
        image["local_path"] = str(target)
        return "skipped"

    try:
        response = get_thread_session().get(image["url"], timeout=30)
        response.raise_for_status()
        target.write_bytes(response.content)
        image["local_path"] = str(target)
        return "downloaded"
    except Exception as exc:
        logger.warning(
            "Не удалось скачать картинку offer=%s file=%s: %s",
            offer_id,
            target.name,
            exc,
        )
        return "failed"


def add_status(stats: dict[str, int], status: str) -> None:
    if status in stats:
        stats[status] += 1


def download_sequential(
    tasks: list[tuple[int, dict[str, Any], Path]], stats: dict[str, int]
) -> None:
    for number, task in enumerate(tasks, start=1):
        if stop_requested:
            break
        add_status(stats, download_one(task))
        if number == 1 or number % 50 == 0 or number == len(tasks):
            logger.info(
                "Прогресс: %s/%s | скачано=%s ошибок=%s пропущено=%s",
                number,
                len(tasks),
                stats["downloaded"],
                stats["failed"],
                stats["skipped"],
            )
        time.sleep(IMAGE_DELAY_SEC)


def download_parallel(
    tasks: list[tuple[int, dict[str, Any], Path]], stats: dict[str, int]
) -> None:
    with ThreadPoolExecutor(max_workers=max(1, IMAGE_WORKERS)) as executor:
        futures = [executor.submit(download_one, task) for task in tasks]
        for number, future in enumerate(as_completed(futures), start=1):
            if stop_requested:
                for item in futures:
                    item.cancel()
                break
            try:
                add_status(stats, future.result())
            except Exception as exc:
                logger.warning("Ошибка worker: %s", exc)
                stats["failed"] += 1
            if number == 1 or number % 100 == 0 or number == len(tasks):
                logger.info(
                    "Прогресс: %s/%s | скачано=%s ошибок=%s пропущено=%s",
                    number,
                    len(tasks),
                    stats["downloaded"],
                    stats["failed"],
                    stats["skipped"],
                )


def main() -> None:
    setup_logging()
    install_signal_handlers()

    offers = load_offers()
    stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    tasks = build_tasks(offers, stats)
    logger.info("К скачиванию: %s файлов, уже есть: %s", len(tasks), stats["skipped"])

    try:
        if IMAGE_WORKERS == 1:
            download_sequential(tasks, stats)
        else:
            download_parallel(tasks, stats)
    finally:
        save_json(offers)
        save_csv(offers)

    status = "остановлено, прогресс сохранён" if stop_requested else "готово"
    logger.info(
        "%s: скачано=%s пропущено=%s ошибок=%s",
        status,
        stats["downloaded"],
        stats["skipped"],
        stats["failed"],
    )
    logger.info("Картинки: %s", IMAGES_DIR.resolve())


if __name__ == "__main__":
    main()
