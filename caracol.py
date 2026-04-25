import requests
import pandas as pd
from scrapy import Selector 
import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path

# Busca el .env en la misma carpeta donde está el script
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=int(os.getenv("DB_PORT"))
)

cursor = conn.cursor()

response = requests.get(
    "https://www.caracol.com.co",
    headers={"User-Agent": "Mozilla/5.0"}
)
html = response.text
sel = Selector(text=html)

# VERSION PARA LEER DESDE ARCHIVO ESTATICO
# html_filepath = "html_dataset/caracol_radio.html"
# with open(html_filepath,"r",encoding="utf-8") as f:
#     html = f.read()

#     sel = Selector(text=html)


articulos = sel.xpath(
    '(//section[@class="c-cad cad-4-col "])[1]//article//header'
).getall()

# Contadores
nuevas = 0
vistas = 0

for articulo in articulos:
    sel_articulo = Selector(text=articulo)
    titulo = sel_articulo.xpath('.//h2/text() | .//h2/a/text()').get()
    url_relativa= sel_articulo.xpath('.//@href').get()
    url_articulo = f"https://www.caracol.com.co{url_relativa}" if url_relativa else None

    if titulo and url_articulo:
        try:
            cursor.execute(
                """
                INSERT INTO noticias (titulo, url, medio, scraped_first, scraped_last)
                VALUES (%s, %s, %s, NOW(), NOW())
                ON CONFLICT (url) DO UPDATE
                    SET scraped_last = NOW(),
                        titulo = EXCLUDED.titulo
                RETURNING (scraped_first = scraped_last) AS es_nueva
                """,
                (titulo.strip(), url_articulo.strip(), "Caracol Radio")
            )

            resultado = cursor.fetchone()
            if resultado[0]:  # type: ignore
                nuevas += 1
            else:
                vistas += 1

        except Exception as e:
            print(f"Error al insertar: {e}")
        
cursor.execute("""
    INSERT INTO scrape_logs (medio, noticias_nuevas, noticias_vistas, status)
    VALUES (%s, %s, %s, 'ok')
""", ("Caracol Radio", nuevas, vistas))

conn.commit()  # guarda los cambios
cursor.close()
conn.close()

print(f"Nuevas: {nuevas} | Ya existían: {vistas}", flush=True)