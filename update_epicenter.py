import os
import requests
import time
import xml.etree.ElementTree as ET
from pathlib import Path
import shutil

# ================== НАСТРОЙКИ ==================
ROZETKA_URL = "http://parser.biz.ua/Aqua/api/export.aspx?action=rozetka&key=ui82P2VotQQamFTj512NQJK3HOlKvyv7"
EPICENTER_URL = "https://aqua-favorit.com.ua/content/export/e8965786f1dc7b09ba9950b66c9f7fba.xml"

TMP_DIR = Path("/tmp/epicenter_feed")
TMP_DIR.mkdir(parents=True, exist_ok=True)

ROZETKA_XML = TMP_DIR / "rozetka.xml"
EPICENTER_XML = TMP_DIR / "epicenter.xml"
OUTPUT_XML = TMP_DIR / "update_epicenter.xml"

# ===== ЧЁРНЫЕ СПИСКИ =====
BANNED_VENDORS = {
    "Ariston", "Atlant", "Bosch", "Bradas", "Franke",
    "Mexen", "Neon", "NoName", "TeploCeramic", "Yoka", "Новая Вода"
}

BANNED_CATEGORY_ROOTS = {
    "1276",  # Декоративные панели для вентиляторов
    "1278",  # Обратные клапана, сифоны
    "1157",  # Душевые кабины / Комплектующие
    "1252",  # Средства герметизации
    "1251",  # Трубы водопроводные
    "1199",  # Фильтры / Комплектующие
    "1161",  # Раковины / Комплектующие
}

# ================== TELEGRAM ==================
TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def send_telegram(message: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("⚠ TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не задан. Сообщение не отправлено.")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠ Ошибка отправки в Telegram: {e}")

# ================== СКАЧИВАНИЕ ==================
def download_file(url, path, title, retries=5, timeout=180):
    print(f"▶ Загрузка: {title}")
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        if chunk:
                            f.write(chunk)
            print(f"  ✅ {title} загружен\n")
            return
        except Exception as e:
            print(f"  ⚠ Ошибка: {e}")
            if attempt == retries:
                raise
            time.sleep(5)

print("\n===== СТАРТ =====\n")
download_file(ROZETKA_URL, ROZETKA_XML, "Розетка XML")
download_file(EPICENTER_URL, EPICENTER_XML, "Эпицентр XML")

# ================== РОЗЕТКА ==================
rozetka_data = {}
tree_r = ET.parse(ROZETKA_XML)
for offer in tree_r.getroot().findall(".//offer"):
    rid = offer.get("id")
    if rid:
        rozetka_data[rid.strip()] = {
            "price": offer.findtext("price", "").strip(),
            "old_price": offer.findtext("oldprice", "").strip(),
            "available": offer.get("available", "").strip()
        }

# ================== ЭПИЦЕНТР ==================
tree = ET.parse(EPICENTER_XML)
root = tree.getroot()

# --- строим карту категорий ---
category_parent = {}
for cat in root.findall(".//category"):
    cid = cat.get("id")
    pid = cat.get("parentId")
    if cid and pid:
        category_parent[cid] = pid

def is_banned_category(cid: str) -> bool:
    while cid:
        if cid in BANNED_CATEGORY_ROOTS:
            return True
        cid = category_parent.get(cid)
    return False

offers_root = root.find(".//offers")
offers = offers_root.findall("offer")
removed = 0

for offer in offers:
    vendor = offer.findtext("vendor", "").strip()
    category_id = offer.findtext("categoryId", "").strip()

    if vendor in BANNED_VENDORS or is_banned_category(category_id):
        offers_root.remove(offer)
        removed += 1
        continue

    vendor_code = offer.findtext("vendorCode", "").strip()
    if not vendor_code:
        continue

    param_artikul = offer.find(".//param[@name='Артикул']")
    if param_artikul is not None and param_artikul.text:
        offer_id = param_artikul.text.strip()
    else:
        offer_id = vendor_code

    offer.set("id", offer_id)

    if offer_id in rozetka_data:
        data = rozetka_data[offer_id]
        if data["price"]:
            offer.find("price").text = data["price"]
        if data["old_price"]:
            old = offer.find("oldprice") or ET.SubElement(offer, "price_old")
            old.tag = "price_old"  # заменяем тег
            old.text = data["old_price"]
        offer.set("available", data["available"])

# ================== СОХРАНЕНИЕ ==================
tree.write(OUTPUT_XML, encoding="UTF-8", xml_declaration=True)
REPO_ROOT = Path.cwd()
shutil.copy2(OUTPUT_XML, REPO_ROOT / "update_epicenter.xml")

# ================== ОТПРАВКА В TELEGRAM ==================
message = f"""===== СТАРТ =====

▶ Загрузка: Розетка XML
  ✅ Розетка XML загружен

▶ Загрузка: Эпицентр XML
  ✅ Эпицентр XML загружен

❌ Удалено из файла (левых) товаров: {removed}
📦 Отправляем на Эпицентр товаров: {len(offers_root.findall('offer'))}
===== ГОТОВО ✅ ====="""

send_telegram(message)
print(message)
