import os
import requests
import time
from pathlib import Path
import shutil
from lxml import etree as ET
import copy

# ================== НАСТРОЙКИ ==================
ROZETKA_URL = "http://parser.biz.ua/Aqua/api/export.aspx?action=rozetka&key=ui82P2VotQQamFTj512NQJK3HOlKvyv7"
EPICENTER_URL = "https://aqua-favorit.com.ua/content/export/e8965786f1dc7b09ba9950b66c9f7fba.xml"

TMP_DIR = Path("/tmp/epicenter_feed")
TMP_DIR.mkdir(parents=True, exist_ok=True)

ROZETKA_XML = TMP_DIR / "rozetka.xml"
EPICENTER_XML = TMP_DIR / "epicenter.xml"
OUTPUT_XML = TMP_DIR / "update_epicenter.xml"

# ===== ЧЁРНЫЕ СПИСКИ =====
BANNED_VENDORS = {"Ariston","Atlant","Bosch","Bradas","Franke","Mexen","Neon","NoName","TeploCeramic","Yoka","Новая Вода"}
BANNED_CATEGORY_ROOTS = {"1276","1278","1157","1252","1251","1199","1161"}

# ================== TELEGRAM ==================
TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def send_telegram(message: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("⚠ TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не задан. Сообщение не отправлено.")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID,"text": message,"parse_mode": "HTML"}
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠ Ошибка отправки в Telegram: {e}")

# ================== СКАЧИВАНИЕ ==================
def download_file(url, path, title, retries=5, timeout=180):
    print(f"▶ Загрузка: {title}")
    for attempt in range(1, retries+1):
        try:
            r = requests.get(url, stream=True, timeout=timeout)
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(1024*1024):
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
tree_r = ET.parse(str(ROZETKA_XML))
for offer in tree_r.xpath("//offer"):
    rid = offer.get("id")
    if rid:
        rozetka_data[rid.strip()] = {
            "price": offer.findtext("price","").strip(),
            "old_price": offer.findtext("oldprice","").strip(),
            "available": offer.get("available","").strip()
        }

# ================== ЭПИЦЕНТР ==================
tree = ET.parse(str(EPICENTER_XML))
root = tree.getroot()

category_parent = {c.get("id"): c.get("parentId") for c in root.xpath("//category")}

def is_banned_category(cid):
    while cid:
        if cid in BANNED_CATEGORY_ROOTS:
            return True
        cid = category_parent.get(cid)
    return False

new_root = ET.Element("yml_catalog", date=root.get("date",""))
new_offers = ET.SubElement(new_root, "offers")

removed = 0
for offer in root.xpath("//offer"):
    vendor = offer.findtext("vendor","").strip()
    category_id = offer.findtext("categoryId","").strip()
    if vendor in BANNED_VENDORS or is_banned_category(category_id):
        removed +=1
        continue

    offer_copy = copy.deepcopy(offer)

    # артикул и id
    vendor_code = offer_copy.findtext("vendorCode","").strip()
    param_artikul = offer_copy.find(".//param[@name='Артикул']")
    if param_artikul is not None and param_artikul.text:
        offer_id = param_artikul.text.strip()
    elif vendor_code:
        offer_id = vendor_code
    else:
        offer_id = offer_copy.get("id")
    offer_copy.set("id", offer_id)

    # обновляем цены и наличие
    if offer_id in rozetka_data:
        data = rozetka_data[offer_id]
        if data["price"]:
            offer_copy.find("price").text = data["price"]
        if data["old_price"]:
            old = offer_copy.find("oldprice") or ET.SubElement(offer_copy, "oldprice")
            old.text = data["old_price"]
        offer_copy.set("available", data["available"])

    # name / description
    name = offer_copy.find("name")
    name_ua = offer_copy.find("name_ua")
    if name is not None:
        name.tag = "name"
        name.set("lang","ru")
    if name_ua is not None:
        name_ua.tag = "name"
        name_ua.set("lang","ua")
    description = offer_copy.find("description")
    description_ua = offer_copy.find("description_ua")
    if description is not None:
        description.tag = "description"
        description.set("lang","ru")
    if description_ua is not None:
        description_ua.tag = "description"
        description_ua.set("lang","ua")

    # oldprice -> price_old
    for oldprice_elem in offer_copy.xpath(".//oldprice"):
        oldprice_elem.tag = "price_old"

    new_offers.append(offer_copy)

# ================== СОХРАНЕНИЕ ==================
tree_new = ET.ElementTree(new_root)
tree_new.write(str(OUTPUT_XML), encoding="UTF-8", xml_declaration=True, pretty_print=True)
shutil.copy2(OUTPUT_XML, Path.cwd() / "update_epicenter.xml")

# ================== TELEGRAM ==================
message = f"""===== СТАРТ =====

▶ Загрузка: Розетка XML
  ✅ Розетка XML загружен

▶ Загрузка: Эпицентр XML
  ✅ Эпицентр XML загружен

❌ Удалено из файла (левых) товаров: {removed}
📦 Отправляем на Эпицентр товаров: {len(new_offers.xpath('offer'))}
===== ГОТОВО ✅ ====="""

send_telegram(message)
print(message)
