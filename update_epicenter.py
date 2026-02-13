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
BANNED_VENDORS = {
    "Ariston","Atlant","Bosch","Bradas","Franke",
    "Mexen","Neon","NoName","TeploCeramic","Yoka","Новая Вода"
}

BANNED_CATEGORY_ROOTS = {"1276","1278","1157","1252","1251","1199","1161"}

# ===== КАТЕГОРИИ ЭПИЦЕНТРА (по корневым ID) =====
EPICENTER_CATEGORY_MAP = {
    "962": "Ванни",
    "963": "Ванни гідромасажні",
    "966": "Шторки для ванн",
    "993": "Змішувачі",
    "974": "Унітази та компакти",
    "983": "Інсталяції",
    "1654": "Сифони",
    "6922": "Душові системи",
    "969": "Душові кабіни",
    "988": "Дзеркала для ванної кімнати",
    "4600": "Мийки для кухні",
    "1619": "Бойлери",
}

# ================== TELEGRAM ==================
TG_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def send_telegram(message: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID,"text": message}
    try:
        requests.post(url, data=payload, timeout=10)
    except:
        pass

# ================== СКАЧИВАНИЕ ==================
def download_file(url, path):
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)

print("\n===== СТАРТ =====\n")
download_file(ROZETKA_URL, ROZETKA_XML)
download_file(EPICENTER_URL, EPICENTER_XML)

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

# строим дерево категорий
category_parent = {c.get("id"): c.get("parentId") for c in root.xpath("//category")}

def find_root_category(cid):
    """Поднимаемся вверх по дереву категорий"""
    while cid:
        if cid in EPICENTER_CATEGORY_MAP:
            return cid
        cid = category_parent.get(cid)
    return None

def is_banned_category(cid):
    while cid:
        if cid in BANNED_CATEGORY_ROOTS:
            return True
        cid = category_parent.get(cid)
    return False

new_root = ET.Element("yml_catalog", date=root.get("date",""))
new_offers = ET.SubElement(new_root, "offers")

removed = 0
exported = 0

for offer in root.xpath("//offer"):

    vendor = offer.findtext("vendor","").strip()
    category_id = offer.findtext("categoryId","").strip()

    if vendor in BANNED_VENDORS or is_banned_category(category_id):
        removed += 1
        continue

    mapped_category = find_root_category(category_id)

    if not mapped_category:
        removed += 1
        continue

    offer_copy = copy.deepcopy(offer)

    # ===== ID =====
    vendor_code = offer_copy.findtext("vendorCode","").strip()
    param_artikul = offer_copy.find(".//param[@name='Артикул']")

    if param_artikul is not None and param_artikul.text:
        offer_id = param_artikul.text.strip()
    elif vendor_code:
        offer_id = vendor_code
    else:
        offer_id = offer_copy.get("id")

    offer_copy.set("id", offer_id)

    # ===== ОБНОВЛЕНИЕ ЦЕН =====
    if offer_id in rozetka_data:
        data = rozetka_data[offer_id]
        if data["price"]:
            offer_copy.find("price").text = data["price"]
        if data["old_price"]:
            old = offer_copy.find("oldprice") or ET.SubElement(offer_copy, "oldprice")
            old.text = data["old_price"]
        offer_copy.set("available", data["available"])

    # ===== NAME / DESCRIPTION =====
    for tag, lang in [("name","ru"), ("name_ua","ua")]:
        elem = offer_copy.find(tag)
        if elem is not None:
            elem.tag = "name"
            elem.set("lang", lang)

    for tag, lang in [("description","ru"), ("description_ua","ua")]:
        elem = offer_copy.find(tag)
        if elem is not None:
            elem.tag = "description"
            elem.set("lang", lang)

    # ===== oldprice → price_old =====
    for oldprice_elem in offer_copy.xpath(".//oldprice"):
        oldprice_elem.tag = "price_old"

    # ===== CATEGORY + ATTRIBUTE_SET =====
    category_name = EPICENTER_CATEGORY_MAP[mapped_category]

    cat_el = ET.Element("category")
    cat_el.set("code", mapped_category)
    cat_el.text = category_name
    offer_copy.append(cat_el)

    attr_el = ET.Element("attribute_set")
    attr_el.set("code", mapped_category)
    attr_el.text = category_name
    offer_copy.append(attr_el)

    new_offers.append(offer_copy)
    exported += 1

# ================== СОХРАНЕНИЕ ==================
tree_new = ET.ElementTree(new_root)
tree_new.write(str(OUTPUT_XML), encoding="UTF-8", xml_declaration=True, pretty_print=True)
shutil.copy2(OUTPUT_XML, Path.cwd() / "update_epicenter.xml")

# ================== TELEGRAM ==================
message = f"""===== ГОТОВО ✅ =====
❌ Удалено товаров: {removed}
📦 Отправляем товаров: {exported}
"""
send_telegram(message)
print(message)
