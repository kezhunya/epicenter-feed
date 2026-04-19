import copy
import csv
import json
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from lxml import etree as ET
from openpyxl import Workbook

# ================== НАСТРОЙКИ ==================
ROZETKA_URL = "http://parser.biz.ua/Aqua/api/export.aspx?action=rozetka&key=ui82P2VotQQamFTj512NQJK3HOlKvyv7"
EPICENTER_URL = "https://aqua-favorit.com.ua/content/export/e8965786f1dc7b09ba9950b66c9f7fba.xml"
GOOGLE_TABLE_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1S6L2iLlvJzDf2vNBoSPRwEuoEIP_muojrT-00KshR_c/gviz/tq?tqx=out:csv&gid=0"
)
GOOGLE_SETTINGS_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1S6L2iLlvJzDf2vNBoSPRwEuoEIP_muojrT-00KshR_c/gviz/tq?tqx=out:csv&sheet=%D0%9D%D0%B0%D1%81%D1%82%D1%80%D0%BE%D0%B9%D0%BA%D0%B8"
)
ROZETKA_DOWNLOAD_TIMEOUT_SEC = int(os.environ.get("ROZETKA_DOWNLOAD_TIMEOUT_SEC", "240"))
EPICENTER_DOWNLOAD_TIMEOUT_SEC = int(os.environ.get("EPICENTER_DOWNLOAD_TIMEOUT_SEC", "180"))
GOOGLE_TABLE_TIMEOUT_SEC = int(os.environ.get("GOOGLE_TABLE_TIMEOUT_SEC", "60"))
GOOGLE_SETTINGS_TIMEOUT_SEC = int(os.environ.get("GOOGLE_SETTINGS_TIMEOUT_SEC", "30"))
GOOGLE_EUR_RATE_FALLBACK = float(os.environ.get("GOOGLE_EUR_RATE_FALLBACK", "52.5"))
SOURCE_STALE_HOURS = int(os.environ.get("SOURCE_STALE_HOURS", "48"))

TMP_DIR = Path("/tmp/epicenter_feed")
TMP_DIR.mkdir(parents=True, exist_ok=True)
ROZETKA_XML = TMP_DIR / "rozetka.xml"
EPICENTER_XML = TMP_DIR / "epicenter.xml"
TMP_OUTPUT_XML = TMP_DIR / "update_epicenter.xml"
OUTPUT_XML = Path(__file__).resolve().parent / "update_epicenter.xml"
BRAND_CODES_JSON = Path(__file__).with_name("brand_codes_171.json")
CATEGORY_PARAM_MAP_JSON = Path(__file__).with_name("category_param_map.json")
OSTATKI_DIR = Path(os.environ.get("OSTATKI_DIR", "/Volumes/X-Files/Загрузки рабочие/Остатки"))
SOURCE_ISSUES_XLSX = OSTATKI_DIR / "Проблема исходников.xlsx"
BACKUP_DIR = Path(os.environ.get("SHARED_BACKUP_DIR", str(OSTATKI_DIR / "Backup")))
LEGACY_BACKUP_DIR = Path(__file__).with_name("backups")
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
LEGACY_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
ROZETKA_BACKUP_XML = BACKUP_DIR / "parserbiz_last.xml"
EPICENTER_BACKUP_XML = BACKUP_DIR / "epicenter_source_last.xml"
GOOGLE_TABLE_BACKUP_CSV = BACKUP_DIR / "google_table_last.csv"
GOOGLE_SETTINGS_BACKUP_JSON = BACKUP_DIR / "google_settings_last.json"
LEGACY_ROZETKA_BACKUP_XML = LEGACY_BACKUP_DIR / "parserbiz_last.xml"
LEGACY_EPICENTER_BACKUP_XML = LEGACY_BACKUP_DIR / "epicenter_last.xml"
LEGACY_GOOGLE_TABLE_BACKUP_CSV = LEGACY_BACKUP_DIR / "google_table_last.csv"
LEGACY_GOOGLE_SETTINGS_BACKUP_JSON = LEGACY_BACKUP_DIR / "google_settings_last.json"
SOURCES_STATE_JSON = BACKUP_DIR / "sources_state_epicenter.json"
ROZETKA_BACKUP_CANDIDATES = [ROZETKA_BACKUP_XML, LEGACY_ROZETKA_BACKUP_XML, ROZETKA_XML]
EPICENTER_BACKUP_CANDIDATES = [EPICENTER_BACKUP_XML, LEGACY_EPICENTER_BACKUP_XML, EPICENTER_XML]
GOOGLE_TABLE_BACKUP_CANDIDATES = [GOOGLE_TABLE_BACKUP_CSV, LEGACY_GOOGLE_TABLE_BACKUP_CSV]
GOOGLE_SETTINGS_BACKUP_CANDIDATES = [GOOGLE_SETTINGS_BACKUP_JSON, LEGACY_GOOGLE_SETTINGS_BACKUP_JSON]
LOCAL_ENV_CANDIDATES = [
    Path(__file__).resolve().parent / ".env",
    Path(__file__).resolve().parent.parent / ".env",
]

# ===== ЧЁРНЫЕ СПИСКИ =====
BANNED_VENDORS = {
    "Ariston",
    "Atlant",
    "Bosch",
    "Bradas",
    "Franke",
    "Mexen",
    "Neon",
    "NoName",
    "TeploCeramic",
    "Yoka",
    "Новая Вода",
}

BANNED_CATEGORY_ROOTS = {"1276", "1278", "1157", "1252", "1251", "1199", "1161"}

# source categoryId -> (Epicenter code, Epicenter leaf category title)
CATEGORY_MAPPING = {
    "1009": ("962", "Ванни"),
    "1059": ("962", "Ванни"),
    "1060": ("962", "Ванни"),
    "1061": ("962", "Ванни"),
    "1062": ("962", "Ванни"),
    "1064": ("963", "Ванни гідромасажні"),
    "1065": ("963", "Ванни гідромасажні"),
    "1066": ("966", "Шторки для ванн"),
    "1200": ("966", "Шторки для ванн"),
    "1067": ("6905", "Монтажні елементи та аксесуари для ванн"),
    "1149": ("6905", "Монтажні елементи та аксесуари для ванн"),
    "1179": ("967", "Ніжки для ванн"),
    "1180": ("967", "Ніжки для ванн"),
    "1178": ("6905", "Монтажні елементи та аксесуари для ванн"),
    "1181": ("6905", "Монтажні елементи та аксесуари для ванн"),
    "1177": ("6905", "Монтажні елементи та аксесуари для ванн"),
    "1150": ("965", "Панелі для ванн"),
    "1007": ("993", "Змішувачі"),
    "1068": ("993", "Змішувачі"),
    "1069": ("993", "Змішувачі"),
    "1070": ("993", "Змішувачі"),
    "1072": ("993", "Змішувачі"),
    "1073": ("993", "Змішувачі"),
    "1071": ("993", "Змішувачі"),
    "1075": ("6924", "Гігієнічний душ"),
    "1211": ("993", "Змішувачі"),
    "1214": ("993", "Змішувачі"),
    "1076": ("993", "Змішувачі"),
    "1170": ("6914", "Аксесуари та комплектуючі для змішувачів"),
    "1217": ("993", "Змішувачі"),
    "1218": ("993", "Змішувачі"),
    "1242": ("1648", "Запірна арматура"),
    "1080": ("974", "Унітази та компакти"),
    "1081": ("983", "Інсталяції"),
    "1082": ("974", "Унітази та компакти"),
    "1083": ("974", "Унітази та компакти"),
    "1084": ("974", "Унітази та компакти"),
    "1174": ("974", "Унітази та компакти"),
    "1085": ("977", "Біде"),
    "1201": ("983", "Інсталяції"),
    "1086": ("978", "Пісуари"),
    "1087": ("980", "Бачки для унітаза"),
    "1088": ("981", "Сидіння та кришки для унітаза"),
    "1089": ("981", "Сидіння та кришки для унітаза"),
    "1090": ("1654", "Сифони"),
    "1175": ("979", "Чаші Генуя"),
    "1094": ("976", "П'єдестали для раковин"),
    "1095": ("976", "П'єдестали для раковин"),
    "1096": ("976", "П'єдестали для раковин"),
    "1097": ("1654", "Сифони"),
    "1098": ("1654", "Сифони"),
    "1101": ("6922", "Душові системи"),
    "1102": ("6922", "Душові системи"),
    "1100": ("6917", "Душові набори"),
    "1099": ("9376", "Верхні та бокові душі"),
    "1109": ("9420", "Кронштейни для душу"),
    "1103": ("6920", "Лійки для душу"),
    "1104": ("6921", "Штанги, тримачі та підключення для душу"),
    "1105": ("6916", "Шланги для душу"),
    "1106": ("6921", "Штанги, тримачі та підключення для душу"),
    "1107": ("6921", "Штанги, тримачі та підключення для душу"),
    "1108": ("9376", "Верхні та бокові душі"),
    "1156": ("6914", "Аксесуари та комплектуючі для змішувачів"),
    "1110": ("969", "Душові кабіни"),
    "1114": ("970", "Гідромасажні бокси"),
    "1111": ("971", "Душові піддони"),
    "1116": ("6636", "Трапи"),
    "1117": ("6636", "Трапи"),
    "1112": ("972", "Душові двері та стінки"),
    "1113": ("972", "Душові двері та стінки"),
    "1118": ("1654", "Сифони"),
    "1157": ("6908", "Комплектуючі та аксесуари для душових кабін та боксів"),
    "1121": ("983", "Інсталяції"),
    "1127": ("983", "Інсталяції"),
    "1122": ("983", "Інсталяції"),
    "1123": ("983", "Інсталяції"),
    "1124": ("983", "Інсталяції"),
    "1125": ("983", "Інсталяції"),
    "1126": ("984", "Клавіші змиву та комплектуючі"),
    "1128": ("984", "Клавіші змиву та комплектуючі"),
    "1129": ("988", "Дзеркала для ванної кімнати"),
    "1133": ("988", "Дзеркала для ванної кімнати"),
    "1130": ("989", "Шафи та пенали для ванної кімнати"),
    "1132": ("987", "Тумби для ванної кімнати"),
    "1131": ("987", "Тумби для ванної кімнати"),
    "1232": ("987", "Тумби для ванної кімнати"),
    "1176": ("3561", "Стільниці і комплектуючі для ванної кімнати"),
    "1202": ("987", "Тумби для ванної кімнати"),
    "1134": ("3561", "Стільниці і комплектуючі для ванної кімнати"),
    "1166": ("4600", "Мийки для кухні"),
    "1224": ("993", "Змішувачі"),
    "1169": ("1654", "Сифони"),
    "1140": ("6508", "Набори аксесуарів"),
    "1142": ("6626", "Гачки та планки для ванної кімнати"),
    "1265": ("6594", "Тримачі для ванної кімнати"),
    "1136": ("6624", "Тримачі для рушників"),
    "1137": ("6620", "Тримачі для туалетного паперу"),
    "1143": ("6873", "Мильниці"),
    "1138": ("6619", "Дозатори для рідкого мила"),
    "1145": ("6543", "Тримачі для зубних щіток"),
    "1144": ("991", "Полиці для ванної кімнати"),
    "1139": ("6629", "Йоржики для унітаза"),
    "1189": ("1002", "Стійки для ванної кімнати"),
    "1135": ("999", "Відра та кошики для ванної кімнати"),
    "1171": ("6854", "Поручні для ванни"),
    "1141": ("6502", "Косметичні дзеркала"),
    "1190": ("999", "Відра та кошики для ванної кімнати"),
    "1205": ("6544", "Сушарки для рук"),
    "1206": ("2788", "Фени"),
    "1264": ("6619", "Дозатори для рідкого мила"),
    "1219": ("6619", "Дозатори для рідкого мила"),
    "1173": ("6908", "Комплектуючі та аксесуари для душових кабін та боксів"),
    "1212": ("999", "Відра та кошики для ванної кімнати"),
    "1191": ("1001", "Килимки для ванної кімнати"),
    "1120": ("1005", "Рушникосушарки електричні"),
    "1119": ("1004", "Рушникосушарки водяні"),
    "1215": ("6919", "Радіатори дизайнерські"),
    "1262": ("1615", "Терморегулююча арматура"),
    "1213": ("1005", "Рушникосушарки електричні"),
    "1167": ("5461", "Комплектуючі для рушникосушарок"),
    "1158": ("3541", "Побутові витяжні вентилятори"),
    "1159": ("3539", "Повітропроводи та монтажні елементи"),
    "1276": ("3540", "Вентиляційні решітки"),
    "1277": ("4949", "Автоматика"),
    "1278": ("3539", "Повітропроводи та монтажні елементи"),
    "1275": ("3544", "Рекуператори"),
    "1228": ("4985", "Теплові панелі"),
    "1227": ("4985", "Теплові панелі"),
    "1226": ("4985", "Теплові панелі"),
    "1266": ("4912", "Інфрачервоні обігрівачі"),
    "1271": ("1625", "Тепла підлога електрична"),
    "1272": ("1625", "Тепла підлога електрична"),
    "1160": ("1619", "Бойлери"),
    "1233": ("1620", "Газові колонки"),
    "1231": ("1604", "Котли газові"),
    "1234": ("1605", "Котли електричні"),
    "1259": ("1606", "Котли твердопаливні"),
    "1235": ("1609", "Комплектуючі для котлів та допоміжне обладнання"),
    "1223": ("1666", "Водопровідні насоси"),
    "1249": ("1648", "Запірна арматура"),
    "1250": ("1648", "Запірна арматура"),
    "1253": ("1647", "Шланги для підключення"),
    "1255": ("7985", "Колектори водопровідні"),
    "1251": ("6607", "Металопластикові труби"),
    "1257": ("5349", "Внутрішня каналізація"),
    "1254": ("6906", "Фітинги різьбові"),
    "1256": ("5349", "Внутрішня каналізація"),
    "1245": ("1640", "Лічильники води"),
    "1273": ("1609", "Комплектуючі для котлів та допоміжне обладнання"),
    "1274": ("1609", "Комплектуючі для котлів та допоміжне обладнання"),
}

# ================== TELEGRAM ==================
def load_local_env(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception as exc:
        print(f"⚠ Не удалось прочитать .env ({path}): {exc}")


for env_path in LOCAL_ENV_CANDIDATES:
    load_local_env(env_path)

TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


def send_telegram(message: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("⚠ TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не задан. Сообщение не отправлено.")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
    except Exception as exc:
        print(f"⚠ Ошибка отправки в Telegram: {exc}")


# ================== СКАЧИВАНИЕ ==================
def download_file(url: str, path: Path, title: str, retries: int = 5, timeout: int = 180) -> None:
    print(f"▶ Загрузка: {title}")
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, stream=True, timeout=timeout)
            response.raise_for_status()
            with open(path, "wb") as file_obj:
                for chunk in response.iter_content(1024 * 1024):
                    if chunk:
                        file_obj.write(chunk)
            print(f" ✅ {title} загружен\n")
            return
        except Exception as exc:
            print(f" ⚠ Ошибка: {exc}")
            if attempt == retries:
                raise
            time.sleep(5)


def backup_date_str(path: Path | None) -> str:
    if path is None:
        return datetime.now().strftime("%d.%m.%Y")
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).strftime("%d.%m.%Y")
    except Exception:
        return datetime.now().strftime("%d.%m.%Y")


def source_status_block(title: str, loaded_from_source: bool, backup_path: Path | None = None) -> str:
    if loaded_from_source:
        return f"▶ Загрузка: {title}\n✅ {title} загружен"
    return f"⛔️ {title} не загружен - взят из backup ({backup_date_str(backup_path)})"


def load_sources_state() -> dict:
    if not SOURCES_STATE_JSON.exists():
        return {}
    try:
        return json.loads(SOURCES_STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_sources_state(state: dict) -> None:
    try:
        SOURCES_STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        print(f"⚠ Не удалось сохранить {SOURCES_STATE_JSON.name}: {exc}")


def update_source_success(state: dict, source_key: str, used_path: Path) -> None:
    state[source_key] = {
        "last_success_ts": int(time.time()),
        "last_success_path": str(used_path),
        "first_failure_ts": None,
        "last_failure_ts": None,
    }


def update_source_failure(state: dict, source_key: str) -> None:
    now_ts = int(time.time())
    item = state.get(source_key, {})
    first_failure = item.get("first_failure_ts")
    if not isinstance(first_failure, int):
        first_failure = now_ts
    item["first_failure_ts"] = first_failure
    item["last_failure_ts"] = now_ts
    state[source_key] = item


def source_stale_info(
    state: dict,
    source_key: str,
    source_label: str,
    backup_path: Path | None = None,
    backup_filename: str = "",
) -> dict | None:
    item = state.get(source_key, {})
    ref_ts: int | None = None
    if isinstance(item.get("last_success_ts"), int):
        ref_ts = int(item["last_success_ts"])
    elif backup_path and backup_path.exists():
        try:
            ref_ts = int(backup_path.stat().st_mtime)
        except Exception:
            ref_ts = None
    if ref_ts is None:
        return None
    stale_hours = (time.time() - ref_ts) / 3600.0
    if stale_hours < SOURCE_STALE_HOURS:
        return None
    backup_dt = datetime.fromtimestamp(ref_ts).strftime("%d.%m.%Y %H:%M:%S")
    backup_hint = backup_filename or (backup_path.name if backup_path else "")
    return {
        "source": source_label,
        "backup_date": backup_dt,
        "manual": "Да",
        "save_hint": f"/Volumes/X-Files/Загрузки рабочие/Остатки/Backup/{backup_hint}",
    }


def write_source_issues_report(rows: list[dict]) -> bool:
    if not rows:
        return False

    try:
        OSTATKI_DIR.mkdir(parents=True, exist_ok=True)
        wb = Workbook()
        ws = wb.active
        ws.title = "Проблема исходников"
        ws.append(
            [
                "название исходника",
                "дата последнего удачного backup",
                "нужно обновить вручную",
                "ссылка на папку куда сохранить и с каким именем",
            ]
        )
        for row in rows:
            ws.append([row["source"], row["backup_date"], row["manual"], row["save_hint"]])
        wb.save(SOURCE_ISSUES_XLSX)
        return True
    except Exception as exc:
        print(f"⚠ Не удалось сформировать {SOURCE_ISSUES_XLSX}: {exc}")
        return False


def now_kyiv() -> datetime:
    return datetime.now(ZoneInfo("Europe/Kyiv"))


def resolve_valid_backup(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        if not candidate.exists() or candidate.stat().st_size == 0:
            continue
        try:
            ET.parse(str(candidate))
        except Exception:
            continue
        return candidate
    return None


def resolve_existing_backup(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate
    return None


def normalize_text(value: str | None) -> str:
    return (value or "").strip()


def normalize_key(value: str | None) -> str:
    return normalize_text(value).casefold()


def normalize_sheet_number(value: str) -> str:
    raw = normalize_text(value).replace("\xa0", "").replace(" ", "")
    if not raw:
        return ""
    cleaned = re.sub(r"[^0-9,.\-]", "", raw).replace(",", ".")
    if cleaned.count(".") > 1:
        parts = cleaned.split(".")
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    try:
        num = float(cleaned)
    except ValueError:
        return ""
    if abs(num - round(num)) < 1e-9:
        return str(int(round(num)))
    return f"{num:.2f}"


def parse_number_float(value: str | None) -> float | None:
    raw = normalize_text(value).replace("\xa0", "").replace(" ", "")
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9,.\-]", "", raw).replace(",", ".")
    if cleaned.count(".") > 1:
        parts = cleaned.split(".")
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(cleaned)
    except ValueError:
        return None


def format_number_for_feed(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    text = f"{value:.2f}"
    return text.rstrip("0").rstrip(".")


def pick_effective_price(source_price: str, external_price: str) -> str:
    source_norm = normalize_sheet_number(source_price)
    external_norm = normalize_sheet_number(external_price)
    if not external_norm:
        return source_norm
    source_num = parse_number_float(source_norm)
    external_num = parse_number_float(external_norm)
    if source_num is not None and external_num is not None and external_num < source_num:
        return source_norm
    return external_norm


def pick_effective_old_price(final_price: str, external_old_price: str) -> str:
    old_norm = normalize_sheet_number(external_old_price)
    if not old_norm:
        return ""
    final_num = parse_number_float(final_price)
    old_num = parse_number_float(old_norm)
    if final_num is None or old_num is None or old_num <= final_num:
        return ""
    return old_norm


def normalize_sheet_available(value: str) -> str:
    key = normalize_key(value).replace("\xa0", " ")
    if key in {"в наличии", "є в наявності", "в наявності", "available"}:
        return "true"
    if key in {"нет в наличии", "немає в наявності", "не в наличии", "false"}:
        return "false"
    return ""


def parse_google_eur_rate(csv_text: str) -> float | None:
    reader = csv.reader(csv_text.splitlines())
    for row in reader:
        if not row:
            continue
        left = normalize_key(row[0] if len(row) > 0 else "")
        middle = normalize_key(row[1] if len(row) > 1 else "")
        if left == "eur" or middle == "eur":
            rate = parse_number_float(row[2] if len(row) > 2 else "")
            if rate and rate > 0:
                return rate
    return None


def load_google_eur_rate() -> float:
    try:
        response = requests.get(GOOGLE_SETTINGS_CSV_URL, timeout=GOOGLE_SETTINGS_TIMEOUT_SEC)
        response.raise_for_status()
        rate = parse_google_eur_rate(response.text)
        if rate and rate > 0:
            GOOGLE_SETTINGS_BACKUP_JSON.write_text(
                json.dumps(
                    {"eur_rate": rate, "fetched_at": now_kyiv().strftime("%Y-%m-%d %H:%M:%S")},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            return rate
        raise ValueError("Не удалось прочитать курс EUR из Настройки")
    except Exception as exc:
        backup = resolve_existing_backup(GOOGLE_SETTINGS_BACKUP_CANDIDATES)
        if backup is not None:
            try:
                payload = json.loads(backup.read_text(encoding="utf-8"))
                rate = float(payload.get("eur_rate", 0))
                if rate > 0:
                    if backup.resolve() != GOOGLE_SETTINGS_BACKUP_JSON.resolve():
                        shutil.copy2(backup, GOOGLE_SETTINGS_BACKUP_JSON)
                    print(f"⚠ Курс EUR из backup настроек: {rate} ({exc})")
                    return rate
            except Exception:
                pass
        print(f"⚠ Курс EUR недоступен, fallback {GOOGLE_EUR_RATE_FALLBACK}: {exc}")
        return GOOGLE_EUR_RATE_FALLBACK


def parse_google_table_index(csv_text: str, eur_rate: float) -> dict[str, dict[str, str]]:
    idx: dict[str, dict[str, str]] = {}
    rows = csv.DictReader(csv_text.splitlines())
    for row in rows:
        normalized_row = {normalize_key(k): normalize_text(v) for k, v in row.items() if k}
        article = normalized_row.get("артикул", "")
        if not article:
            continue
        key = normalize_key(article)
        if not key or key in idx:
            continue

        raw_price = normalize_sheet_number(normalized_row.get("цена ррц", ""))
        raw_old_price = normalize_sheet_number(normalized_row.get("старая цена", ""))
        currency = normalize_key(normalized_row.get("валюта", ""))
        if currency in {"eur", "планета воды"}:
            price_num = parse_number_float(raw_price)
            old_num = parse_number_float(raw_old_price)
            price = format_number_for_feed((price_num or 0.0) * eur_rate) if price_num is not None else ""
            old_price = format_number_for_feed(old_num * eur_rate) if old_num is not None else ""
        else:
            price = raw_price
            old_price = raw_old_price

        idx[key] = {
            "price": price,
            "old_price": old_price,
            "available": normalize_sheet_available(normalized_row.get("наличие", "")),
        }
    return idx


def load_google_table_index(sources_state: dict | None = None) -> tuple[dict[str, dict[str, str]], bool, Path | None]:
    eur_rate = load_google_eur_rate()
    try:
        response = requests.get(GOOGLE_TABLE_CSV_URL, timeout=GOOGLE_TABLE_TIMEOUT_SEC)
        response.raise_for_status()
        idx = parse_google_table_index(response.text, eur_rate)
        if not idx:
            raise ValueError("Google CSV пустой или без колонки Артикул")
        GOOGLE_TABLE_BACKUP_CSV.write_text(response.text, encoding="utf-8")
        if sources_state is not None:
            update_source_success(sources_state, "google_sheet", GOOGLE_TABLE_BACKUP_CSV)
        return idx, True, None
    except Exception as exc:
        backup = resolve_existing_backup(GOOGLE_TABLE_BACKUP_CANDIDATES)
        if backup is None:
            print(f"⚠ Google Table недоступна, fallback отключен: {exc}")
            if sources_state is not None:
                update_source_failure(sources_state, "google_sheet")
            return {}, False, None
        try:
            idx = parse_google_table_index(backup.read_text(encoding="utf-8"), eur_rate)
        except UnicodeDecodeError:
            idx = parse_google_table_index(backup.read_text(encoding="utf-8-sig"), eur_rate)
        if not idx:
            print(f"⚠ Google Table недоступна, backup пустой: {backup} ({exc})")
            if sources_state is not None:
                update_source_failure(sources_state, "google_sheet")
            return {}, False, backup
        if backup.resolve() != GOOGLE_TABLE_BACKUP_CSV.resolve():
            shutil.copy2(backup, GOOGLE_TABLE_BACKUP_CSV)
        print(f"⚠ Google Table недоступна, используем backup: {backup} ({exc})")
        if sources_state is not None:
            update_source_failure(sources_state, "google_sheet")
        return idx, False, backup


def resolve_offer_id_raw(offer: ET._Element) -> str:
    for param in offer.xpath(".//param"):
        if normalize_key(param.get("name")) == "артикул":
            value = normalize_text(param.text)
            if value:
                return value
    vendor_code = normalize_text(offer.findtext("vendorCode", ""))
    if vendor_code:
        return vendor_code
    return normalize_text(offer.get("id"))


def resolve_offer_id_key(offer: ET._Element) -> str:
    return normalize_key(resolve_offer_id_raw(offer))


def clear_oldprice_nodes(offer: ET._Element) -> bool:
    changed = False
    for node in list(offer):
        if node.tag in {"oldprice", "old_price", "price_old"}:
            offer.remove(node)
            changed = True
    return changed


print("\n===== СТАРТ =====\n")
sources_state = load_sources_state()
rozetka_loaded_from_source = True
rozetka_fallback_path: Path | None = None
try:
    download_file(
        ROZETKA_URL,
        ROZETKA_XML,
        "Розетка XML",
        timeout=ROZETKA_DOWNLOAD_TIMEOUT_SEC,
    )
    shutil.copy2(ROZETKA_XML, ROZETKA_BACKUP_XML)
    update_source_success(sources_state, "parserbiz", ROZETKA_XML)
except Exception as exc:
    backup = resolve_valid_backup(ROZETKA_BACKUP_CANDIDATES)
    if backup is None:
        raise exc
    rozetka_loaded_from_source = False
    rozetka_fallback_path = backup
    if backup.resolve() != ROZETKA_XML.resolve():
        shutil.copy2(backup, ROZETKA_XML)
    if backup.resolve() != ROZETKA_BACKUP_XML.resolve():
        shutil.copy2(backup, ROZETKA_BACKUP_XML)
    update_source_failure(sources_state, "parserbiz")
    print(f"⚠ Розетка недоступна, используем backup: {backup}")

epicenter_loaded_from_source = True
epicenter_fallback_path: Path | None = None
try:
    download_file(
        EPICENTER_URL,
        EPICENTER_XML,
        "Эпицентр XML",
        timeout=EPICENTER_DOWNLOAD_TIMEOUT_SEC,
    )
    shutil.copy2(EPICENTER_XML, EPICENTER_BACKUP_XML)
    update_source_success(sources_state, "epicenter_source", EPICENTER_XML)
except Exception as exc:
    backup = resolve_valid_backup(EPICENTER_BACKUP_CANDIDATES)
    if backup is None:
        raise exc
    epicenter_loaded_from_source = False
    epicenter_fallback_path = backup
    if backup.resolve() != EPICENTER_XML.resolve():
        shutil.copy2(backup, EPICENTER_XML)
    if backup.resolve() != EPICENTER_BACKUP_XML.resolve():
        shutil.copy2(backup, EPICENTER_BACKUP_XML)
    update_source_failure(sources_state, "epicenter_source")
    print(f"⚠ Эпицентр недоступен, используем backup: {backup}")

# ================== РОЗЕТКА ==================
rozetka_data = {}
tree_r = ET.parse(str(ROZETKA_XML))
for offer in tree_r.xpath("//offer"):
    rid = resolve_offer_id_key(offer)
    if rid:
        rozetka_data[rid] = {
            "price": offer.findtext("price", "").strip(),
            "old_price": offer.findtext("oldprice", "").strip(),
            "available": offer.get("available", "").strip(),
        }
google_table_idx, _google_loaded_from_source, _google_fallback_path = load_google_table_index(sources_state)
save_sources_state(sources_state)
stale_issue_rows: list[dict] = []
stale_defs = [
    ("parserbiz", "Исходник Parser.biz", resolve_valid_backup(ROZETKA_BACKUP_CANDIDATES), "parserbiz_last.xml"),
    ("epicenter_source", "Исходник Epicenter", resolve_valid_backup(EPICENTER_BACKUP_CANDIDATES), "epicenter_source_last.xml"),
    ("google_sheet", "Google Sheet", resolve_existing_backup(GOOGLE_TABLE_BACKUP_CANDIDATES), "google_table_last.csv"),
]
for source_key, source_label, backup_path, backup_filename in stale_defs:
    info = source_stale_info(
        sources_state,
        source_key,
        source_label,
        backup_path=backup_path,
        backup_filename=backup_filename,
    )
    if info:
        stale_issue_rows.append(info)
backup_issue_active = write_source_issues_report(stale_issue_rows)

# ================== ЭПИЦЕНТР ==================
tree = ET.parse(str(EPICENTER_XML))
root = tree.getroot()
category_parent = {cat.get("id"): cat.get("parentId") for cat in root.xpath("//category")}

COUNTRY_PARAM_NAMES = {
    "страна регистрации бренда",
    "країна реєстрації бренду",
    "страна бренда",
    "країна бренду",
}

COUNTRY_CODE_MAP = {
    "украина": "ukr",
    "україна": "ukr",
    "польша": "pol",
    "польща": "pol",
    "китай": "chn",
    "китай (кнр)": "chn",
    "чехия": "cze",
    "чехія": "cze",
    "германия": "deu",
    "німеччина": "deu",
    "италия": "ita",
    "італія": "ita",
    "испания": "esp",
    "іспанія": "esp",
    "турция": "tur",
    "туреччина": "tur",
    "португалия": "prt",
    "португалія": "prt",
    "нидерланды": "nld",
    "нідерланди": "nld",
    "швейцария": "che",
    "швейцарія": "che",
    "румыния": "rou",
    "румунія": "rou",
    "болгария": "bgr",
    "болгарія": "bgr",
    "сербия": "srb",
    "сербія": "srb",
    "польща.": "pol",
}


def load_epicenter_dicts(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    if not path.exists():
        return {}, {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}, {}

    brand = {
        str(name).strip().casefold(): str(code).strip()
        for name, code in (raw.get("brand") or {}).items()
        if str(name).strip() and str(code).strip()
    }
    country = {
        str(name).strip().casefold(): str(code).strip()
        for name, code in (raw.get("country") or {}).items()
        if str(name).strip() and str(code).strip()
    }
    return brand, country


BRAND_CODE_MAP, COUNTRY_CODE_MAP_XLSX = load_epicenter_dicts(BRAND_CODES_JSON)


def load_category_param_map(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    result = {}
    for cat_id, cfg in (raw or {}).items():
        if not isinstance(cfg, dict):
            continue
        name = str(cfg.get("name", "")).strip()
        paramcode = str(cfg.get("paramcode", "")).strip()
        value = str(cfg.get("value", "")).strip()
        valuecode = str(cfg.get("valuecode", "")).strip()
        if not (name and paramcode and value):
            continue
        result[str(cat_id).strip()] = {
            "name": name,
            "paramcode": paramcode,
            "value": value,
            "valuecode": valuecode,
        }
    return result


CATEGORY_PARAM_MAP = load_category_param_map(CATEGORY_PARAM_MAP_JSON)


def is_banned_category(category_id: str) -> bool:
    while category_id:
        if category_id in BANNED_CATEGORY_ROOTS:
            return True
        category_id = category_parent.get(category_id)
    return False


def find_param_value(offer: ET._Element, names: set[str]) -> str:
    normalized_names = {name.strip().casefold() for name in names}
    for param in offer.xpath(".//param"):
        pname = (param.get("name") or "").strip().casefold()
        if pname in normalized_names:
            return (param.text or "").strip()
    return ""


def country_code_from_name(country_name: str) -> str:
    normalized = country_name.strip().casefold()
    normalized = normalized.replace(".", "")
    xlsx_code = COUNTRY_CODE_MAP_XLSX.get(normalized)
    if xlsx_code:
        return xlsx_code
    return COUNTRY_CODE_MAP.get(normalized, "unk")


def vendor_code_from_name(vendor_name: str) -> str:
    normalized = vendor_name.strip().casefold()
    if not normalized:
        return ""
    return BRAND_CODE_MAP.get(normalized, "")


new_root = ET.Element("yml_catalog", date=now_kyiv().strftime("%Y-%m-%d %H:%M"))
new_offers = ET.SubElement(new_root, "offers")

removed = 0
mapped = 0
unmapped = 0
duplicate_ids_removed = 0
seen_offer_ids = set()
category_params_added = 0

for offer in root.xpath("//offer"):
    vendor = offer.findtext("vendor", "").strip()
    src_category_id = offer.findtext("categoryId", "").strip()

    if vendor in BANNED_VENDORS or is_banned_category(src_category_id):
        removed += 1
        continue

    offer_copy = copy.deepcopy(offer)
    offer_copy.attrib.pop("group_id", None)
    url_node = offer_copy.find("url")
    if url_node is not None:
        offer_copy.remove(url_node)

    # артикул -> id
    vendor_code = offer_copy.findtext("vendorCode", "").strip()
    param_artikul = offer_copy.find(".//param[@name='Артикул']")

    if param_artikul is not None and (param_artikul.text or "").strip():
        offer_id = param_artikul.text.strip()
    elif vendor_code:
        offer_id = vendor_code
    else:
        offer_id = (offer_copy.get("id") or "").strip()

    if offer_id:
        offer_copy.set("id", offer_id)
        if offer_id in seen_offer_ids:
            duplicate_ids_removed += 1
            continue
        seen_offer_ids.add(offer_id)

    vendor_node = offer_copy.find("vendor")
    if vendor_node is not None:
        computed_vendor_code = vendor_code_from_name(vendor_node.text or "")
        if computed_vendor_code:
            vendor_node.set("code", computed_vendor_code)
        else:
            vendor_node.attrib.pop("code", None)
    for vendor_code_node in offer_copy.findall("vendorCode"):
        offer_copy.remove(vendor_code_node)

    country_name = find_param_value(offer_copy, COUNTRY_PARAM_NAMES)
    if country_name:
        country_code = country_code_from_name(country_name)
        country_node = offer_copy.find("country_of_origin")
        if country_node is None:
            country_node = ET.Element("country_of_origin", code=country_code)
            country_node.text = country_name
            if vendor_node is not None:
                vendor_index = list(offer_copy).index(vendor_node)
                offer_copy.insert(vendor_index + 1, country_node)
            else:
                offer_copy.append(country_node)
        else:
            country_node.set("code", country_code)
            country_node.text = country_name

    # Цена: Rozetka/Google с защитой от занижения, Наличие: только из AquaFavorit.
    source_price = normalize_text(offer.findtext("price", ""))
    source_available = normalize_text(offer.get("available"))
    offer_key = normalize_key(offer_id)
    rz_data = rozetka_data.get(offer_key)
    google_row = google_table_idx.get(offer_key)
    selected_price = source_price
    selected_old_price = ""

    if rz_data:
        selected_price = pick_effective_price(source_price, rz_data.get("price", ""))
        selected_old_price = pick_effective_old_price(selected_price, rz_data.get("old_price", ""))
    elif google_row:
        selected_price = pick_effective_price(source_price, google_row.get("price", ""))
        selected_old_price = pick_effective_old_price(selected_price, google_row.get("old_price", ""))

    price_node = offer_copy.find("price")
    if price_node is not None and selected_price:
        price_node.text = selected_price
    if selected_old_price:
        old = offer_copy.find("oldprice")
        if old is None:
            old = ET.SubElement(offer_copy, "oldprice")
        old.text = selected_old_price
    else:
        clear_oldprice_nodes(offer_copy)
    if source_available:
        offer_copy.set("available", source_available)

    # name / description -> lang tags
    name = offer_copy.find("name")
    name_ua = offer_copy.find("name_ua")
    if name is not None:
        name.tag = "name"
        name.set("lang", "ru")
    if name_ua is not None:
        name_ua.tag = "name"
        name_ua.set("lang", "ua")

    description = offer_copy.find("description")
    description_ua = offer_copy.find("description_ua")
    if description is not None:
        description.tag = "description"
        description.set("lang", "ru")
    if description_ua is not None:
        description_ua.tag = "description"
        description_ua.set("lang", "ua")

    # Epicenter не принимает currencyId в offer
    for currency_node in offer_copy.findall("currencyId"):
        offer_copy.remove(currency_node)

    # Оборачиваем description в CDATA, чтобы HTML внутри не парсился как XML-разметка
    for desc_node in offer_copy.findall("description"):
        parts = []
        if desc_node.text:
            parts.append(desc_node.text)
        for child in list(desc_node):
            parts.append(ET.tostring(child, encoding="unicode"))
            desc_node.remove(child)
        desc_html = "".join(parts).strip()
        if desc_html:
            desc_node.text = ET.CDATA(desc_html)

    # oldprice -> price_old
    for oldprice_elem in offer_copy.xpath(".//oldprice"):
        oldprice_elem.tag = "price_old"

    # categoryId -> category + attribute_set (Epicenter format)
    mapped_category = CATEGORY_MAPPING.get(src_category_id)
    category_id_node = offer_copy.find("categoryId")
    if category_id_node is not None:
        offer_copy.remove(category_id_node)

    if mapped_category:
        mapped += 1
        ep_code, ep_name = mapped_category
    else:
        unmapped += 1
        ep_code = src_category_id or "0"
        ep_name = "Без категорії"

    category_node = ET.Element("category", code=ep_code)
    category_node.text = ep_name
    attribute_set_node = ET.Element("attribute_set", code=ep_code)
    attribute_set_node.text = ep_name

    insert_pos = 2 if len(offer_copy) >= 2 else len(offer_copy)
    offer_copy.insert(insert_pos, category_node)
    offer_copy.insert(insert_pos + 1, attribute_set_node)

    # убрать все param по требованию: не передавать параметры товаров
    for param in offer_copy.xpath(".//param"):
        parent = param.getparent()
        if parent is not None:
            parent.remove(param)

    # Подкатегория из исходника -> параметр Epicenter (через локальный маппинг)
    param_cfg = CATEGORY_PARAM_MAP.get(src_category_id)
    if param_cfg:
        param_node = ET.Element("param", name=param_cfg["name"], paramcode=param_cfg["paramcode"])
        if param_cfg["valuecode"]:
            param_node.set("valuecode", param_cfg["valuecode"])
        param_node.text = param_cfg["value"]
        offer_copy.append(param_node)
        category_params_added += 1

    new_offers.append(offer_copy)

# ================== СОХРАНЕНИЕ ==================
tree_new = ET.ElementTree(new_root)
tree_new.write(str(TMP_OUTPUT_XML), encoding="UTF-8", xml_declaration=True, pretty_print=False)
shutil.copy2(TMP_OUTPUT_XML, OUTPUT_XML)
size_mb = OUTPUT_XML.stat().st_size / (1024 * 1024)

# ================== TELEGRAM ==================
source_header = "\n".join(
    [
        source_status_block("Розетка XML", rozetka_loaded_from_source, rozetka_fallback_path),
        source_status_block("Эпицентр XML", epicenter_loaded_from_source, epicenter_fallback_path),
    ]
)
backup_issue_line = "⛔️Необходимо обновить Backup!!!\n\n" if backup_issue_active else ""

message = f"""===== 🛠ЭПИЦЕНТР🛠=====
{source_header}
{backup_issue_line}

❌ Удалено из файла (левых) товаров: {removed}
🆔 Удалено дублей по offer id: {duplicate_ids_removed}
🗂 Сопоставлено категорий: {mapped}
⚠ Не найдено категорий в таблице: {unmapped}
🧷 Добавлено параметров подкатегории: {category_params_added}

📦 Отправляем на Эпицентр товаров: {len(new_offers.xpath('offer'))}
📐 Размер итогового файла: {size_mb:.2f} MB
===== ГОТОВО ✅ ====="""

send_telegram(message)
print(message)
