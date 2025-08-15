import os
import psycopg2
from psycopg2 import Error
from psycopg2 import errors
from datetime import datetime
import logging
import streamlit as st

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@st.cache_resource(ttl=3600)
def get_db_connection():
    db_host = os.environ.get("DB_HOST", "localhost")
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    db_port = os.environ.get("DB_PORT", "6543")

    if not all([db_host, db_name, db_user, db_password]):
        logging.warning("ADVERTENCIA: Una o más variables de entorno de la base de datos no están configuradas. Intentando conectar con valores predeterminados o vacíos.")

    try:
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password, port=db_port)
        logging.info("Conexión a la DB establecida exitosamente (usando caché).")
        return conn
    except Error as e:
        logging.error(f"Error al conectar a la base de datos PostgreSQL: {e}")
        return None

class DatabaseConnection:
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = get_db_connection()
        
        if self.conn and self.conn.closed != 0:
            logging.warning("Conexión en caché encontrada, pero está cerrada. Recreando la conexión.")
            # Si la conexión está cerrada, borra la caché y obtiene una nueva.
            get_db_connection.clear()
            self.conn = get_db_connection()
        
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
                logging.error(f"Transacción revertida debido a un error: {exc_val}")

def crear_tablas():
    try:
        with DatabaseConnection() as conn:
            if conn is None:
                logging.error("No se pudo obtener una conexión a la base de datos.")
                return
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS usuarios (
                        id SERIAL PRIMARY KEY,
                        username TEXT NOT NULL UNIQUE,
                        password TEXT NOT NULL, -- NOTA: En un entorno real, esto DEBE ser un hash de la contraseña.
                        role TEXT NOT NULL
                    );
                """)
                cursor.execute("""
                    INSERT INTO usuarios (username, password, role) VALUES ('legalizador', 'legalizador123', 'legalizador')
                    ON CONFLICT (username) DO NOTHING;
                """)
                cursor.execute("""
                    INSERT INTO usuarios (username, password, role) VALUES ('auditor', 'auditor123', 'auditor')
                    ON CONFLICT (username) DO NOTHING;
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS facturas (
                        id SERIAL PRIMARY KEY,
                        numero_factura TEXT NOT NULL,
                        area_servicio TEXT,
                        facturador TEXT,
                        fecha_generacion DATE,
                        eps TEXT,
                        fecha_hora_entrega TIMESTAMP,
                        tiene_correccion BOOLEAN DEFAULT FALSE,
                        descripcion_devolucion TEXT,
                        fecha_devolucion_lider DATE,
                        revisado BOOLEAN DEFAULT FALSE,
                        factura_original_id INTEGER,
                        estado TEXT DEFAULT 'Activa',
                        reemplazada_por_numero_factura TEXT,
                        estado_auditoria TEXT DEFAULT 'Pendiente', -- Valor por defecto inicial
                        observacion_auditor TEXT,
                        tipo_error TEXT,
                        fecha_reemplazo DATE,
                        fecha_entrega_radicador TIMESTAMP,
                        FOREIGN KEY (factura_original_id) REFERENCES facturas(id),
                        CONSTRAINT unique_factura_details UNIQUE (numero_factura, facturador, eps, area_servicio)
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS detalles_soat (
                        id SERIAL PRIMARY KEY,
                        factura_id INTEGER UNIQUE,
                        fecha_generacion_soat DATE,
                        FOREIGN KEY (factura_id) REFERENCES facturas(id) ON DELETE CASCADE
                    );
                """)
                
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_facturas_numero_factura ON facturas (numero_factura);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_facturas_facturador ON facturas (facturador);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_facturas_eps ON facturas (eps);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_facturas_area_servicio ON facturas (area_servicio);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_facturas_estado_auditoria ON facturas (estado_auditoria);")

                logging.info("Tablas verificadas/creadas, usuarios predeterminados e índices insertados.")
    except Error as e:
        logging.error(f"Error al crear tablas, insertar usuarios o índices: {e}")

def obtener_credenciales_usuario(username):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return None
            with conn.cursor() as cursor:
                cursor.execute("SELECT password, role FROM usuarios WHERE username = %s;", (username,))
                user_data = cursor.fetchone()
                logging.info(f"Credenciales de usuario obtenidas para '{username}'.")
                return user_data
    except Error as e:
        logging.error(f"Error al obtener credenciales del usuario '{username}': {e}")
        return None

def guardar_factura(numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return None
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
                """, (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega))
                factura_id = cursor.fetchone()[0]
                logging.info(f"Factura '{numero_factura}' guardada con ID: {factura_id}")
                return factura_id
    except errors.UniqueViolation as e:
        logging.warning(f"Intento de guardar factura duplicada. La combinación (Número de Factura: '{numero_factura}', Legalizador: '{facturador}', EPS: '{eps}', Área de Servicio: '{area_servicio}') ya existe.")
        return None
    except Error as e:
        logging.error(f"Error al guardar factura '{numero_factura}': {e}")
        return None

def guardar_detalles_soat(factura_id, fecha_generacion_soat):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return False
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO detalles_soat (factura_id, fecha_generacion_soat)
                    VALUES (%s, %s);
                """, (factura_id, fecha_generacion_soat))
                logging.info(f"Detalles SOAT guardados para factura ID: {factura_id}")
                return True
    except Error as e:
        logging.error(f"Error al guardar detalles SOAT para factura ID: {factura_id}: {e}")
        return False

def obtener_factura_por_id(factura_id):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return None
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        f.id, f.numero_factura, f.area_servicio, f.facturador, f.fecha_generacion, f.eps,
                        f.fecha_hora_entrega, f.tiene_correccion, f.descripcion_devolucion,
                        f.fecha_devolucion_lider, f.revisado, f.factura_original_id, f.estado,
                        f.reemplazada_por_numero_factura, f.estado_auditoria, f.observacion_auditor,
                        f.tipo_error, f.fecha_reemplazo, f.fecha_entrega_radicador,
                        fo.numero_factura AS num_fact_original_linked,
                        fo.fecha_generacion AS fecha_gen_original_linked
                    FROM facturas f
                    LEFT JOIN facturas fo ON f.factura_original_id = fo.id
                    WHERE f.id = %s;
                """, (factura_id,))
                
                column_names = [desc[0] for desc in cursor.description]
                factura_data_tuple = cursor.fetchone()
                
                if factura_data_tuple:
                    factura_data = dict(zip(column_names, factura_data_tuple))
                    logging.info(f"Factura ID: {factura_id} obtenida.")
                    return factura_data
                return None
    except Error as e:
        logging.error(f"Error al obtener factura por ID {factura_id}: {e}")
        return None

def obtener_factura_por_numero(numero_factura):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return None
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        id, numero_factura, area_servicio, facturador, fecha_generacion, eps,
                        fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
                        fecha_devolucion_lider, revisado, factura_original_id, estado,
                        reemplazada_por_numero_factura, estado_auditoria, observacion_auditor,
                        tipo_error, fecha_reemplazo, fecha_entrega_radicador
                    FROM facturas
                    WHERE numero_factura = %s;
                """, (numero_factura,))
                
                column_names = [desc[0] for desc in cursor.description]
                factura_data_tuple = cursor.fetchone()
                
                if factura_data_tuple:
                    factura_data = dict(zip(column_names, factura_data_tuple))
                    logging.info(f"Factura con número: {numero_factura} obtenida.")
                    return factura_data
                return None
    except Error as e:
        logging.error(f"Error al obtener factura por número {numero_factura}: {e}")
        return None

def obtener_detalles_soat_por_factura_id(factura_id):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return None
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, factura_id, fecha_generacion_soat
                    FROM detalles_soat
                    WHERE factura_id = %s;
                """, (factura_id,))
                
                column_names = [desc[0] for desc in cursor.description]
                soat_details_tuple = cursor.fetchone()

                if soat_details_tuple:
                    soat_details = dict(zip(column_names, soat_details_tuple))
                    logging.info(f"Detalles SOAT para factura ID: {factura_id} obtenidos.")
                    return soat_details
                return None
    except Error as e:
        logging.error(f"Error al obtener detalles SOAT por factura ID {factura_id}: {e}")
        return None

def actualizar_factura(factura_id, numero_factura, area_servicio, facturador, fecha_generacion, eps,
                       fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
                       fecha_devolucion_lider, revisado, factura_original_id, estado,
                       reemplazada_por_numero_factura, estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return False
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE facturas SET
                        numero_factura = %s, area_servicio = %s, facturador = %s, fecha_generacion = %s, eps = %s,
                        fecha_hora_entrega = %s, tiene_correccion = %s, descripcion_devolucion = %s,
                        fecha_devolucion_lider = %s, revisado = %s, factura_original_id = %s, estado = %s,
                        reemplazada_por_numero_factura = %s, estado_auditoria = %s, observacion_auditor = %s,
                        tipo_error = %s, fecha_reemplazo = %s
                    WHERE id = %s;
                """, (numero_factura, area_servicio, facturador, fecha_generacion, eps,
                      fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
                      fecha_devolucion_lider, revisado, factura_original_id, estado,
                      reemplazada_por_numero_factura, estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo,
                      factura_id))
                if cursor.rowcount == 0:
                    logging.warning(f"No se encontró la factura ID: {factura_id} para actualizar o no hubo cambios.")
                    return False
                logging.info(f"Factura ID: {factura_id} actualizada correctamente.")
                return True
    except errors.UniqueViolation as e:
        logging.warning(f"Intento de actualizar factura con combinación duplicada: (Número: '{numero_factura}', Legalizador: '{facturador}', EPS: '{eps}', Área: '{area_servicio}')")
        return False
    except Error as e:
        logging.error(f"Error al actualizar factura ID: {factura_id}: {e}")
        return False

def actualizar_estado_auditoria_factura(factura_id, nuevo_estado_auditoria, observacion, tipo_error):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return False
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE facturas SET
                        estado_auditoria = %s, observacion_auditor = %s, tipo_error = %s
                    WHERE id = %s;
                """, (nuevo_estado_auditoria, observacion, tipo_error, factura_id))
                logging.info(f"Estado de auditoría de factura ID: {factura_id} actualizado a '{nuevo_estado_auditoria}'.")
                return True
    except Error as e:
        logging.error(f"Error al actualizar estado de auditoría de factura ID: {factura_id}: {e}")
        return False

def actualizar_fecha_entrega_radicador(factura_id, fecha_entrega):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return False
            with conn.cursor() as cursor:
                cursor.execute("SELECT estado_auditoria FROM facturas WHERE id = %s;", (factura_id,))
                current_estado = cursor.fetchone()[0]

                new_estado_auditoria = current_estado
                if fecha_entrega is not None:
                    if current_estado == 'Lista para Radicar':
                        new_estado_auditoria = 'En Radicador'
                else:
                    if current_estado == 'En Radicador':
                        new_estado_auditoria = 'Lista para Radicar'

                cursor.execute("""
                    UPDATE facturas SET fecha_entrega_radicador = %s, estado_auditoria = %s WHERE id = %s;
                """, (fecha_entrega, new_estado_auditoria, factura_id))
                logging.info(f"Fecha de entrega al radicador para factura ID: {factura_id} actualizada. Nuevo estado: {new_estado_auditoria}")
                return True
    except Error as e:
        logging.error(f"Error al actualizar fecha de entrega al radicador para factura ID: {factura_id}: {e}")
        return False

def entregar_facturas_radicador(factura_ids, fecha_entrega):
    try:
        if not factura_ids:
            logging.info("No se proporcionaron IDs para entrega masiva al radicador.")
            return 0

        with DatabaseConnection() as conn:
            if conn is None:
                logging.error("No se pudo obtener conexión para entrega masiva al radicador.")
                return 0
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE facturas SET
                        fecha_entrega_radicador = %s,
                        estado_auditoria = CASE
                            WHEN estado_auditoria = 'Lista para Radicar' AND %s IS NOT NULL THEN 'En Radicador'
                            WHEN estado_auditoria = 'En Radicador' AND %s IS NULL THEN 'Lista para Radicar'
                            ELSE estado_auditoria
                        END
                    WHERE id = ANY(%s);
                """, (fecha_entrega, fecha_entrega, fecha_entrega, factura_ids))
                updated_count = cursor.rowcount
                logging.info(f"Entrega masiva al radicador: {updated_count} facturas actualizadas.")
                return updated_count
    except Error as e:
        logging.error(f"Error en entrega masiva al radicador: {e}")
        return 0

def eliminar_factura(factura_id):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return False
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM facturas WHERE id = %s;", (factura_id,))
                logging.info(f"Factura ID: {factura_id} eliminada correctamente.")
                return True
    except Error as e:
        logging.error(f"Error al eliminar factura ID: {factura_id}: {e}")
        return False

def guardar_factura_reemplazo(old_factura_id, new_numero_factura, fecha_reemplazo):
    try:
        with DatabaseConnection() as conn:
            if conn is None:
                return False
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE facturas SET
                        estado = 'Reemplazada',
                        reemplazada_por_numero_factura = %s,
                        fecha_reemplazo = %s,
                        estado_auditoria = 'Pendiente'
                    WHERE id = %s;
                """, (new_numero_factura, fecha_reemplazo, old_factura_id))
                
                logging.info(f"Factura ID: {old_factura_id} actualizada como reemplazada con el nuevo número: {new_numero_factura}.")
                return True
    except errors.UniqueViolation as e:
        logging.warning(f"Intento de actualizar factura con combinación duplicada: (Número: '{new_numero_factura}')")
        return False
    except Error as e:
        logging.error(f"Error al actualizar factura de reemplazo para ID original {old_factura_id}: {e}")
        return False

def cargar_facturas(search_term=None, search_column=None):
    try:
        with DatabaseConnection() as conn:
            if conn is None: return []
            with conn.cursor() as cursor:
                query = """
                    SELECT
                        f.id, f.numero_factura, f.area_servicio, f.facturador, f.fecha_generacion, f.eps,
                        f.fecha_hora_entrega, f.tiene_correccion, f.descripcion_devolucion,
                        f.fecha_devolucion_lider, f.revisado, f.factura_original_id, f.estado,
                        f.reemplazada_por_numero_factura, f.estado_auditoria, f.observacion_auditor,
                        f.tipo_error, f.fecha_reemplazo, f.fecha_entrega_radicador,
                        fo.numero_factura AS num_fact_original_linked,
                        fo.fecha_generacion AS fecha_gen_original_linked
                    FROM facturas f
                    LEFT JOIN facturas fo ON f.factura_original_id = fo.id
                """
                params = []
                if search_term and search_column:
                    query += f" WHERE f.{search_column} ILIKE %s"
                    params.append(f"%{search_term}%")
                query += " ORDER BY f.id DESC;"
                
                cursor.execute(query, tuple(params))
                
                column_names = [desc[0] for desc in cursor.description]
                facturas_raw = cursor.fetchall()
                facturas = [dict(zip(column_names, row)) for row in facturas_raw]
                
                logging.info(f"Cargadas {len(facturas)} facturas.")
                return facturas
    except Error as e:
        logging.error(f"Error al cargar facturas: {e}")
        return []

def obtener_conteo_facturas_por_legalizador_y_eps():
    try:
        with DatabaseConnection() as conn:
            if conn is None: return []
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT facturador, eps, COUNT(id)
                    FROM facturas
                    WHERE estado_auditoria = 'Pendiente'
                    GROUP BY facturador, eps
                    ORDER BY facturador, eps;
                """)
                stats = cursor.fetchall()
                logging.info("Conteo de facturas pendientes por legalizador y EPS obtenido.")
                return stats
    except Error as e:
        logging.error(f"Error al obtener estadísticas de facturas pendientes: {e}")
        return []

def obtener_conteo_facturas_lista_para_radicar():
    try:
        with DatabaseConnection() as conn:
            if conn is None: return 0
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(id) FROM facturas WHERE estado_auditoria = 'Lista para Radicar';")
                count = cursor.fetchone()[0]
                logging.info(f"Conteo de facturas Lista para Radicar: {count}")
                return count
    except Error as e:
        logging.error(f"Error al obtener conteo de facturas Lista para Radicar: {e}")
        return 0

def obtener_conteo_facturas_en_radicador():
    try:
        with DatabaseConnection() as conn:
            if conn is None: return 0
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(id) FROM facturas WHERE estado_auditoria = 'En Radicador';")
                count = cursor.fetchone()[0]
                logging.info(f"Conteo de facturas En Radicador: {count}")
                return count
    except Error as e:
        logging.error(f"Error al obtener conteo de facturas En Radicador: {e}")
        return 0

def obtener_conteo_facturas_con_errores():
    try:
        with DatabaseConnection() as conn:
            if conn is None: return 0
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(id) FROM facturas
                    WHERE estado_auditoria IN ('Devuelta por Auditor', 'Corregida por Legalizador');
                """)
                count = cursor.fetchone()[0]
                logging.info(f"Conteo de facturas con errores: {count}")
                return count
    except Error as e:
        logging.error(f"Error al obtener conteo de facturas con errores: {e}")
        return 0

def obtener_conteo_facturas_pendientes_global():
    try:
        with DatabaseConnection() as conn:
            if conn is None: return 0
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(id) FROM facturas WHERE estado_auditoria = 'Pendiente';")
                count = cursor.fetchone()[0]
                logging.info(f"Conteo total de facturas pendientes: {count}")
                return count
    except Error as e:
        logging.error(f"Error al obtener conteo total de facturas pendientes: {e}")
        return 0

def obtener_conteo_facturas_vencidas():
    try:
        with DatabaseConnection() as conn:
            if conn is None: return 0
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(id) FROM facturas
                    WHERE estado = 'Vencidas' AND estado_auditoria NOT IN ('Devuelta por Auditor', 'Corregida por Legalizador', 'En Radicador');
                """)
                count = cursor.fetchone()[0]
                logging.info(f"Conteo de facturas vencidas: {count}")
                return count
    except Error as e:
        logging.error(f"Error al obtener conteo de facturas vencidas: {e}")
        return 0

def obtener_conteo_total_facturas():
    try:
        with DatabaseConnection() as conn:
            if conn is None: return 0
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(id) FROM facturas;")
                count = cursor.fetchone()[0]
                logging.info(f"Conteo total de facturas: {count}")
                return count
    except Error as e:
        logging.error(f"Error al obtener conteo total de facturas: {e}")
        return 0

def obtener_facturadores_unicos():
    try:
        with DatabaseConnection() as conn:
            if conn is None: return []
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT facturador FROM facturas WHERE facturador IS NOT NULL ORDER BY facturador;")
                facturadores = [row[0] for row in cursor.fetchall()]
                logging.info("Facturadores únicos obtenidos.")
                return facturadores
    except Error as e:
        logging.error(f"Error al obtener facturadores únicos: {e}")
        return []

def obtener_eps_unicas():
    try:
        with DatabaseConnection() as conn:
            if conn is None: return []
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT eps FROM facturas WHERE eps IS NOT NULL ORDER BY eps;")
                epss = [row[0] for row in cursor.fetchall()]
                logging.info("EPS únicas obtenidas.")
                return epss
    except Error as e:
        logging.error(f"Error al obtener EPS únicas: {e}")
        return []
