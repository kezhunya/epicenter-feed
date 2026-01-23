import requests
import time
import xml.etree.ElementTree as ET
from pathlib import Path

# ================== НАСТРОЙКИ ==================
ROZETKA_URL = "http://parser.biz.ua/Aqua/api/export.aspx?action=rozetka&key=ui82P2VotQQamFTj512NQJK3HOlKvyv7"
EPICENTER_URL = "https://aqua-favorit.com.ua/content/export/7a16de3b4a426940e529447a293728c9.xml"

SAVE_DIR = Path("/tmp/epicenter_feed")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

ROZETKA_XML = SAVE_DIR / "rozetka.xml"
EPICENTER_XML = SAVE_DIR / "epicenter.xml"
OUTPUT_XML = SAVE_DIR / "update_epicenter.xml"

# ================== СКАЧИВАНИЕ ФАЙЛОВ ==================
def download_file(url, path, title, retries=5, timeout=180):
    print(f"▶ Загрузка: {title}")
    for attempt in range(1, retries + 1):
        try:
            print(f"  ⏳ Попытка {attempt}")
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

print("\n===== СТАРТ СКРИПТА =====\n")
download_file(ROZETKA_URL, ROZETKA_XML, "Розетка XML")
download_file(EPICENTER_URL, EPICENTER_XML, "Эпицентр XML")

# ================== ПАРСИНГ РОЗЕТКА ==================
print("▶ Парсинг Розетка...")
rozetka_data = {}

tree = ET.parse(ROZETKA_XML)
root = tree.getroot()

for offer in root.findall(".//offer"):
    rid = offer.get("id")
    if not rid:
        continue
    rozetka_data[rid.strip()] = {
        "price": offer.findtext("price", "").strip(),
        "old_price": offer.findtext("oldprice", "").strip(),
        "available": offer.get("available", "").strip()
    }

print(f"  ✅ Розетка: {len(rozetka_data)} товаров\n")

# ================== ПАРСИНГ ЭПИЦЕНТР И ОБНОВЛЕНИЕ ==================
print("▶ Парсинг Эпицентр и обновление...")
tree = ET.parse(EPICENTER_XML)
root = tree.getroot()

offers = root.findall(".//offer")
for offer in offers:
    vendor_code = offer.findtext("vendorCode", "").strip()
    if not vendor_code:
        continue

    # Проверяем param name="Артикул"
    param_artikul = offer.find(".//param[@name='Артикул']")
    if param_artikul is not None and param_artikul.text.strip() != vendor_code:
        new_id = param_artikul.text.strip()
    else:
        new_id = vendor_code

    offer.set("id", new_id)

    # ===== Подставляем данные из Розетки =====
    if vendor_code in rozetka_data:
        rozetka_info = rozetka_data[vendor_code]

        # available
        offer.set("available", rozetka_info["available"])

        # price — всегда из Розетки
        price_el = offer.find("price")
        if price_el is None:
            price_el = ET.SubElement(offer, "price")
        price_el.text = rozetka_info["price"]

        # oldprice — только если есть
        if rozetka_info["old_price"]:
            oldprice_el = offer.find("oldprice")
            if oldprice_el is None:
                oldprice_el = ET.SubElement(offer, "oldprice")
            oldprice_el.text = rozetka_info["old_price"]
        else:
            oldprice_el = offer.find("oldprice")
            if oldprice_el is not None:
                offer.remove(oldprice_el)

print(f"  ✅ Эпицентр обновлён: {len(offers)} товаров\n")

# ================== СОХРАНЕНИЕ ==================
tree.write(OUTPUT_XML, encoding="UTF-8", xml_declaration=True)
print(f"▶ XML сохранён: {OUTPUT_XML}")

# ================== ОЧИСТКА ==================
ROZETKA_XML.unlink(missing_ok=True)
EPICENTER_XML.unlink(missing_ok=True)

print("===== ФАЙЛ ЭПИЦЕНТР готов !! =====")
