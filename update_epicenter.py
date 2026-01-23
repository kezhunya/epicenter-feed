import requests
import xml.etree.ElementTree as ET
from pathlib import Path

# ================== НАСТРОЙКИ ==================
ROZETKA_URL = "http://parser.biz.ua/Aqua/api/export.aspx?action=rozetka&key=ui82P2VotQQamFTj512NQJK3HOlKvyv7"
EPICENTER_URL = "https://aqua-favorit.com.ua/content/export/7a16de3b4a426940e529447a293728c9.xml"

SAVE_DIR = Path("/tmp")  # временная папка Actions
ROZETKA_XML = SAVE_DIR / "rozetka.xml"
EPICENTER_XML = SAVE_DIR / "epicenter.xml"
OUTPUT_XML = SAVE_DIR / "epicenter.xml"  # финальный файл для публикации

# ================== ЗАГРУЗКА ФАЙЛОВ ==================
def download(url, path):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(1024*1024):
                f.write(chunk)

download(ROZETKA_URL, ROZETKA_XML)
download(EPICENTER_URL, EPICENTER_XML)

# ================== ПАРСИНГ ==================
rozetka_data = {}
tree = ET.parse(ROZETKA_XML)
for offer in tree.findall(".//offer"):
    rid = offer.get("id")
    if rid:
        rozetka_data[rid.strip()] = {
            "price": offer.findtext("price", "").strip(),
            "old_price": offer.findtext("oldprice", "").strip(),
            "available": offer.get("available", "").strip()
        }

tree = ET.parse(EPICENTER_XML)
root = tree.getroot()

for offer in root.findall(".//offer"):
    vendor_code = offer.findtext("vendorCode", "").strip()
    if vendor_code and vendor_code in rozetka_data:
        r = rozetka_data[vendor_code]
        offer.find("price").text = r["price"]
        # если нет oldprice в исходнике
        if offer.find("oldprice") is None:
            ET.SubElement(offer, "oldprice").text = r["old_price"]
        else:
            offer.find("oldprice").text = r["old_price"]
        offer.set("available", r["available"])

tree.write(OUTPUT_XML, encoding="utf-8", xml_declaration=True)
