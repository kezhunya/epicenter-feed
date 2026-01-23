import requests
import time
import xml.etree.ElementTree as ET
from pathlib import Path
import shutil

# ================== TELEGRAM ==================
TG_BOT_TOKEN = "PASTE_YOUR_BOT_TOKEN_HERE"
TG_CHAT_ID = "PASTE_YOUR_CHAT_ID_HERE"

LOG = []

def log(msg):
    print(msg)
    LOG.append(msg)

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    requests.post(url, data={
        "chat_id": TG_CHAT_ID,
        "text": message
    }, timeout=20)

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
    "1276", "1278", "1157", "1252",
    "1251", "1199", "1161",
}

# ================== СКАЧИВАНИЕ ==================
def download_file(url, path, title, retries=5, timeout=180):
    log(f"▶ Загрузка: {title}")
    for _ in range(retries):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(1024 * 1024):
                        if chunk:
                            f.write(chunk)
            log(f"  ✅ {title} загружен\n")
            return
        except Exception as e:
            time.sleep(5)
    raise RuntimeError(f"Не удалось загрузить {title}")

# ================== СТАРТ ==================
log("===== СТАРТ =====\n")

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

category_parent = {}
for cat in root.findall(".//category"):
    cid = cat.get("id")
    pid = cat.get("parentId")
    if cid and pid:
        category_parent[cid] = pid

def is_banned_category(cid):
    while cid:
        if cid in BANNED_CATEGORY_ROOTS:
            return True
        cid = category_parent.get(cid)
    return False

offers = root.findall(".//offer")
removed = 0

for offer in offers[:]:
    vendor = offer.findtext("vendor", "").strip()
    category_id = offer.findtext("categoryId", "").strip()

    if vendor in BANNED_VENDORS or is_banned_category(category_id):
        root.find(".//offers").remove(offer)
        removed += 1
        continue

    vendor_code = offer.findtext("vendorCode", "").strip()
    param = offer.find(".//param[@name='Артикул']")
    offer_id = param.text.strip() if param is not None and param.text else vendor_code
    offer.set("id", offer_id)

    if offer_id in rozetka_data:
        data = rozetka_data[offer_id]
        if data["price"]:
            offer.find("price").text = data["price"]
        if data["old_price"]:
            (offer.find("oldprice") or ET.SubElement(offer, "oldprice")).text = data["old_price"]
        offer.set("available", data["available"])

remaining = len(root.findall(".//offer"))

log(f"❌ Удалено из файла (левых) товаров: {removed}")
log(f"📦 Отправляем на Эпицентр товаров: {remaining}")
log("===== ГОТОВО ✅ =====")

# ================== СОХРАНЕНИЕ ==================
tree.write(OUTPUT_XML, encoding="UTF-8", xml_declaration=True)
shutil.copy2(OUTPUT_XML, Path.cwd() / "update_epicenter.xml")

# ================== TELEGRAM ==================
send_telegram("\n".join(LOG))
