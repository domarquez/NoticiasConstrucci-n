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

# Fuentes bolivianas con énfasis en Santa Cruz (selectores revisados)
FUENTES = [
    {
        "nombre": "El Deber",
        "url": "https://eldeber.com.bo/santa-cruz",
        "selector_titular": "h3.teaser-title, h2.headline, .article-title",
        "selector_resumen": "p.teaser-text, p.summary, .article-excerpt",
        "selector_imagen": "img.teaser-image, img.featured-image, img.article-image",
        "selector_enlace": "a.teaser-link, h3 a, a.article-link"
    },
    {
        "nombre": "El Día",
        "url": "https://www.eldia.com.bo/",
        "selector_titular": "h2.entry-title, h3.post-title, .post-title",
        "selector_resumen": "p.entry-summary, p.description, .excerpt",
        "selector_imagen": "img.post-thumbnail, img.wp-post-image, .featured-image img",
        "selector_enlace": "a.post-url, h2 a, .read-more"
    },
    {
        "nombre": "Cadecocruz",
        "url": "https://cadecocruz.org.bo/index.php?pg2=210",
        "selector_titular": "h2.news-title, h3.article-title, .title",
        "selector_resumen": "p.news-summary, p.article-excerpt, .summary",
        "selector_imagen": "img.news-img, img.article-image, .featured-img",
        "selector_enlace": "a.news-link, a.article-link, a.read-more"
    },
    {
        "nombre": "Contacto Construcción",
        "url": "https://contactoconstruccion.com/",
        "selector_titular": "h2.post-title, h2.entry-title, .article-title",
        "selector_resumen": "p.post-excerpt, p.entry-summary, .excerpt",
        "selector_imagen": "img.post-thumbnail, img.featured, .article-image",
        "selector_enlace": "a.post-url, a.read-more, .article-link"
    },
    {
        "nombre": "Urgente.bo",
        "url": "https://www.urgente.bo/",
        "selector_titular": "h3.article-title, h2.news-title, .title",
        "selector_resumen": "p.article-summary, p.excerpt, .summary",
        "selector_imagen": "img.article-img, img.news-image, .featured-img",
        "selector_enlace": "a.read-more, a.article-link, h3 a"
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
    relevante = any(palabra.lower() in texto_lower for palabra in PALABRAS_CLAVE) or not texto  # Temporalmente relaja el filtro
    logging.debug(f"Texto: '{texto}' - Relevante: {relevante} - Palabras clave: {PALABRAS_CLAVE}")
    return relevante

# Extraer noticias de una fuente (con depuración
