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

# Fuentes ampliadas para toda Bolivia
FUENTES = [
    {
        "nombre": "Contacto Construcción",
        "url": "https://contactoconstruccion.com/",
        "selector_titular": "h4 a, h2.post-title, .article-title",
        "selector_resumen": "h4 + p, p.post-excerpt, .entry-summary",
        "selector_imagen": "img.post-thumbnail, img.featured, .article-image",
        "selector_enlace": "h4 a, a.post-url, a[href*='contactoconstruccion.com/']"
    },
    {
        "nombre": "El Deber",
        "url": "https://eldeber.com.bo/economia",
        "selector_titular": "h3.article-title, h2.headline, .article-title, h2 a, h3 a",
        "selector_resumen": "p.article-summary, p.summary, .article-excerpt, p",
        "selector_imagen": "img.article-image, img.featured-image, .article-thumb img, img",
        "selector_enlace": "a.article-link, h3 a, a[href*='/economia/']"
    },
    {
        "nombre": "Los Tiempos",
        "url": "https://www.lostiempos.com/seccion/economia/1",
        "selector_titular": "h3 a, .article-title, h2 a",
        "selector_resumen": "p.summary, .excerpt, p",
        "selector_imagen": "img.article-image, img.featured, .thumb img",
        "selector_enlace": "a.article-link, h3 a, a[href*='/actualidad/economia/']"
    },
    {
        "nombre": "Opinión",
        "url": "https://www.opinion.com.bo/seccion/economia/1",
        "selector_titular": "h3 a, .title a, h2 a",
        "selector_resumen": "p.excerpt, .summary, p",
        "selector_imagen": "img.featured, .thumb img, img",
        "selector_enlace": "a[href*='/articulo/economia/'], h3 a"
    },
    {
        "nombre": "El País Bolivia",
        "url": "https://elpais.bo/economia/",
        "selector_titular": "h2.headline, h3 a, .article-title",
        "selector_resumen": "p.description, .summary, p",
        "selector_imagen": "img.photo, .featured-image img, img",
        "selector_enlace": "a.link, h2 a, a[href*='/economia/']"
    },
    {
        "nombre": "OOPP.gob.bo",
        "url": "https://www.oopp.gob.bo/notas-de-prensa/",
        "selector_titular": "h3, .noticia-title, h2 a",
        "selector_resumen": "p, .noticia-summary",
        "selector_imagen": "img.noticia-img, .featured-img, img",
        "selector_enlace": "a.noticia-link, h3 a, a[href*='/nota_prensa/']"
    },
    {
        "nombre": "Noticias Fides (ANF)",
        "url": "https://www.noticiasfides.com/economia",
        "selector_titular": "h3.article-title, h2.headline, .title a",
        "selector_resumen": "p.summary, .excerpt, p",
        "selector_imagen": "img.article-image, img.featured, .thumb img",
        "selector_enlace": "a.article-link, h3 a, a[href*='/economia/']"
    },
    {
        "nombre": "BNamericas",
        "url": "https://www.bnamericas.com/en/news/infrastructure?country=Bolivia",
        "selector_titular": "h2.title, h3 a, .news-title",
        "selector_resumen": "p.summary, .excerpt, p",
        "selector_imagen": "img.news-img, .featured, img",
        "selector_enlace": "a.news-link, h2 a, a[href*='/news/infrastructure']"
    },
]

# Palabras clave para noticias nacionales
PALABRAS_CLAVE = ["construcción", "ingeniería", "infraestructura", "Bolivia", "obra", "proyecto", "urbanismo", "vial", "noticia", "nacional", "gobierno", "inversión", "sostenibilidad", "cemento", "inmobiliario", "licitación", "túnel", "carretera"]

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

# Filtrar artículo por relevancia
def es_relevante(texto):
    texto_lower = texto.lower() if texto else ""
    relevante = any(palabra.lower() in texto_lower for palabra in PALABRAS_CLAVE) or not texto
    logging.debug(f"Texto: '{texto}' - Relevante: {relevante} - Palabras clave: {PALABRAS_CLAVE}")
    return relevante

# Extraer noticias de una fuente
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
                logging.debug(f"Intentando guardar: {articulo['titular']} - {articulo['enlace']}")
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
    logging.info(f"Total guardados: {len(todos_articulos)} noticias sobre Bolivia")

# Programar ejecución diaria a las 08:00 -04 (12:00 UTC)
schedule.every().day.at("12:00").do(extraer_todas_las_fuentes)

# Ejecutar el scheduler en un loop continuo
def main():
    logging.info("Iniciando agregador de noticias bolivianas (ejecución continua)...")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
