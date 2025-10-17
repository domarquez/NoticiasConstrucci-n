import requests
from bs4 import BeautifulSoup
import psycopg2
from urllib.parse import urljoin
import schedule
import time
from datetime import datetime
import re
import os
import logging

# Configurar logging para Railway
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Obtener la cadena de conexión desde la variable de entorno
DATABASE_URL = os.getenv("DATABASE_URL")

# Fuentes bolivianas con énfasis en Santa Cruz (selectores ajustados)
FUENTES = [
    {
        "nombre": "El Deber",
        "url": "https://eldeber.com.bo/santa-cruz",
        "selector_titular": "h3.teaser-title, h2.headline",
        "selector_resumen": "p.teaser-text, p.summary",
        "selector_imagen": "img.teaser-image, img.featured-image",
        "selector_enlace": "a.teaser-link, h3 a"
    },
    {
        "nombre": "El Día",
        "url": "https://www.eldia.com.bo/",
        "selector_titular": "h2.entry-title, h3.post-title",
        "selector_resumen": "p.entry-summary, p.description",
        "selector_imagen": "img.post-thumbnail, img.wp-post-image",
        "selector_enlace": "a.post-url, h2 a"
    },
    {
        "nombre": "Cadecocruz",
        "url": "https://cadecocruz.org.bo/index.php?pg2=210",
        "selector_titular": "h2.news-title, h3.article-title",
        "selector_resumen": "p.news-summary, p.article-excerpt",
        "selector_imagen": "img.news-img, img.article-image",
        "selector_enlace": "a.news-link, a.article-link"
    },
    {
        "nombre": "Contacto Construcción",
        "url": "https://contactoconstruccion.com/",
        "selector_titular": "h2.post-title, h2.entry-title",
        "selector_resumen": "p.post-excerpt, p.entry-summary",
        "selector_imagen": "img.post-thumbnail, img.featured",
        "selector_enlace": "a.post-url, a.read-more"
    },
    {
        "nombre": "Urgente.bo",
        "url": "https://www.urgente.bo/",
        "selector_titular": "h3.article-title, h2.news-title",
        "selector_resumen": "p.article-summary, p.excerpt",
        "selector_imagen": "img.article-img, img.news-image",
        "selector_enlace": "a.read-more, a.article-link"
    },
]

# Palabras clave para filtrar noticias relevantes (relajadas para pruebas)
PALABRAS_CLAVE = ["construcción", "ingeniería", "infraestructura", "Santa Cruz", "Bolivia", "obra", "proyecto", "urbanismo", "Urubó", "vial", "noticia"]

# Conexión a Neon
def conectar_db():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logging.info("Conexión exitosa a la base de datos Neon")
        return conn
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        return None

# Crear tabla si no existe
def crear_tabla():
    conn = conectar_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS noticias_construccion_bolivia (
                    id SERIAL PRIMARY KEY,
                    titular TEXT NOT NULL,
                    resumen TEXT,
                    url_imagen TEXT,
                    enlace TEXT UNIQUE,
                    fuente TEXT,
                    fecha_publicacion TIMESTAMP
                );
            """)
            conn.commit()
            logging.info("Tabla creada o verificada correctamente")
            cursor.close()
        except Exception as e:
            logging.error(f"Error al crear la tabla: {e}")
        finally:
            conn.close()

# Filtrar artículo por relevancia (con depuración)
def es_relevante(texto):
    texto_lower = texto.lower()
    relevante = any(palabra.lower() in texto_lower for palabra in PALABRAS_CLAVE)
    logging.debug(f"Texto: '{texto}' - Relevante: {relevante}")
    return relevante

# Extraer noticias de una fuente (con depuración)
def extraer_fuente(fuente):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(fuente["url"], headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        articulos = []
        items = soup.select(fuente["selector_titular"])
        logging.info(f"Elementos encontrados con {fuente['selector_titular']}: {len(items)}")
        for item in items[:5]:  # Limitar a 5 por fuente
            titular = item.get_text(strip=True)
            logging.debug(f"Titular crudo: '{titular}'")
            if not es_relevante(titular):
                continue
            elemento_articulo = item.find_parent("article") or item
            resumen_elem = elemento_articulo.select_one(fuente["selector_resumen"])
            resumen = resumen_elem.get_text(strip=True) if resumen_elem else ""
            logging.debug(f"Resumen crudo: '{resumen}'")
            if not es_relevante(resumen):
                continue
            imagen = elemento_articulo.select_one(fuente["selector_imagen"])
            url_imagen = urljoin(fuente["url"], imagen["src"]) if imagen and imagen.get("src") else ""
            enlace_elem = elemento_articulo.select_one(fuente["selector_enlace"])
            enlace = urljoin(fuente["url"], enlace_elem["href"]) if enlace_elem and enlace_elem.get("href") else ""
            logging.debug(f"Enlace generado: '{enlace}'")

            articulos.append({
                "titular": titular,
                "resumen": resumen[:200] + "..." if len(resumen) > 200 else resumen,
                "url_imagen": url_imagen,
                "enlace": enlace,
                "fuente": fuente["nombre"],
                "fecha_publicacion": datetime.now()
            })
        logging.info(f"Extraídos {len(articulos)} artículos de {fuente['nombre']}")
        return articulos
    except Exception as e:
        logging.error(f"Error al extraer de {fuente['nombre']}: {e}")
        return []

# Guardar artículos en la base de datos (con depuración)
def guardar_en_db(articulos):
    conn = conectar_db()
    if conn:
        try:
            cursor = conn.cursor()
            for articulo in articulos:
                logging.debug(f"Guardando: {articulo['titular']} - {articulo['enlace']}")
                cursor.execute("""
                    INSERT INTO noticias_construccion_bolivia (titular, resumen, url_imagen, enlace, fuente, fecha_publicacion)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (enlace) DO NOTHING;
                """, (
                    articulo["titular"],
                    articulo["resumen"],
                    articulo["url_imagen"],
                    articulo["enlace"],
                    articulo["fuente"],
                    articulo["fecha_publicacion"]
                ))
            conn.commit()
            logging.info(f"Guardados {len(articulos)} artículos en la base de datos")
            cursor.close()
        except Exception as e:
            logging.error(f"Error al guardar en la base de datos: {e}")
        finally:
            conn.close()

# Extraer todas las fuentes
def extraer_todas_las_fuentes():
    crear_tabla()
    todos_articulos = []
    for fuente in FUENTES:
        articulos = extraer_fuente(fuente)
        todos_articulos.extend(articulos)
    guardar_en_db(todos_articulos)
    logging.info(f"Total guardados: {len(todos_articulos)} noticias sobre Bolivia/Santa Cruz")

# Programar ejecución diaria
schedule.every().day.at("08:00").do(extraer_todas_las_fuentes)

# Ejecutar el scheduler
def main():
    logging.info("Iniciando agregador de noticias bolivianas (énfasis Santa Cruz)...")
    extraer_todas_las_fuentes()  # Ejecutar inmediatamente para pruebas
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
