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
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

# Obtener la cadena de conexión desde la variable de entorno
DATABASE_URL = os.getenv("DATABASE_URL")

# Fuentes bolivianas con énfasis en Santa Cruz (selectores ajustados basados en análisis HTML)
FUENTES = [
    {
        "nombre": "El Deber",
        "url": "https://eldeber.com.bo/santa-cruz",
        "selector_titular": "h3.article-title, h2.headline, .article-title",
        "selector_resumen": "p.article-summary, p.summary, .article-excerpt",
        "selector_imagen": "img.article-image, img.featured-image, .article-thumb img",
        "selector_enlace": "a.article-link, h3 a, a[href*='/santa-cruz/']"
    },
    {
        "nombre": "El Día",
        "url": "https://www.eldia.com.bo/",
        "selector_titular": "h3.article-title, h2.entry-title, .post-title",
        "selector_resumen": "p.article-summary, p.entry-summary, .description",
        "selector_imagen": "img.post-thumbnail, img.wp-post-image, .article-image",
        "selector_enlace": "a.article-link, h3 a, a[href*='/YYYY-MM-DD/']"
    },
    {
        "nombre": "Cadecocruz",
        "url": "https://cadecocruz.org.bo/index.php?pg2=210",
        "selector_titular": "h4, h3.news-title, .title",
        "selector_resumen": "p.news-summary, p.article-excerpt, .summary",
        "selector_imagen": "img.news-img, img.article-image, .featured-img",
        "selector_enlace": "a.news-link, a[href*='?op=51&nw='], a.article-link"
    },
    {
        "nombre": "Contacto Construcción",
        "url": "https://contactoconstruccion.com/",
        "selector_titular": "h4 a, h2.post-title, .article-title",
        "selector_resumen": "h4 + p, p.post-excerpt, .entry-summary",
        "selector_imagen": "img.post-thumbnail, img.featured, .article-image",
        "selector_enlace": "h4 a, a.post-url, a[href*='contactoconstruccion.com/']"
    },
    {
        "nombre": "Urgente.bo",
        "url": "https://www.urgente.bo/",
        "selector_titular": "h3.article-title, h2.news-title, .title",
        "selector_resumen": "p.article-summary, p.excerpt, .summary",
        "selector_imagen": "img.article-img, img.news-image, .featured-img",
        "selector_enlace": "a.article-link, h3 a, a[href*='/noticia/']"
    },
]

# Palabras clave para filtrar noticias relevantes (relajadas para pruebas)
PALABRAS_CLAVE = ["construcción", "ingeniería", "infraestructura", "Santa Cruz", "Bolivia", "obra", "proyecto", "urbanismo", "Urubó", "vial", "noticia", "santa", "cruz"]

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

# Filtrar artículo por relevancia (con depuración avanzada)
def es_relevante(texto):
    texto_lower = texto.lower() if texto else ""
    relevante = any(palabra.lower() in texto_lower for palabra in PALABRAS_CLAVE) or not texto  # Relaja el filtro temporalmente
    logging.debug(f"Texto: '{texto}' - Relevante: {relevante} - Palabras clave: {PALABRAS_CLAVE}")
    return relevante

# Extraer noticias de una fuente (con depuración avanzada)
def extraer_fuente(fuente):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        logging.info(f"Iniciando solicitud a {fuente['url']}")
        response = requests.get(fuente["url"], headers=headers, timeout=15)
        response.raise_for_status()
        logging.info(f"Solicitud exitosa a {fuente['url']} - Estado: {response.status_code}")
        soup = BeautifulSoup(response.text, "html.parser")

        articulos = []
        items = soup.select(fuente["selector_titular"])
        logging.info(f"Elementos encontrados con {fuente['selector_titular']}: {len(items)}")
        if not items:
            logging.warning(f"No se encontraron elementos con {fuente['selector_titular']} en {fuente['url']}")
            return []

        for item in items[:5]:
            titular = item.get_text(strip=True)
            logging.debug(f"Titular crudo: '{titular}'")
            if not es_relevante(titular):
                logging.debug(f"Titular descartado por relevancia: '{titular}'")
                continue
            elemento_articulo = item.find_parent("article") or item.find_parent("div", class_=re.compile("article|post|news|teaser|item"))
            if not elemento_articulo:
                logging.warning(f"No se encontró elemento padre para titular: '{titular}'")
                continue
            resumen_elem = elemento_articulo.select_one(fuente["selector_resumen"])
            resumen = resumen_elem.get_text(strip=True) if resumen_elem else ""
            logging.debug(f"Resumen crudo: '{resumen}'")
            if not es_relevante(resumen):
                logging.debug(f"Resumen descartado por relevancia: '{resumen}'")
                continue
            imagen = elemento_articulo.select_one(fuente["selector_imagen"])
            url_imagen = urljoin(fuente["url"], imagen["src"]) if imagen and imagen.get("src") else ""
            enlace_elem = elemento_articulo.select_one(fuente["selector_enlace"]) or item.find("a")
            enlace = urljoin(fuente["url"], enlace_elem["href"]) if enlace_elem and enlace_elem.get("href") else ""
            logging.debug(f"Enlace generado: '{enlace}'")
            if not enlace:
                logging.warning(f"Enlace inválido para titular: '{titular}'")
                continue

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
    except requests.RequestException as e:
        logging.error(f"Error de red al extraer de {fuente['nombre']}: {e}")
        return []
    except Exception as e:
        logging.error(f"Error inesperado al extraer de {fuente['nombre']}: {e}")
        return []

# Guardar artículos en la base de datos
def guardar_en_db(articulos):
    conn = conectar_db()
    if conn:
        try:
            cursor = conn.cursor()
            for articulo in articulos:
                logging.debug(f"Intentando guardar: {articulo['
