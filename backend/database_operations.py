import os
import psycopg2
from psycopg2 import Error
from psycopg2 import errors # Importar errores específicos de psycopg2
from datetime import datetime
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    """
    Establece y retorna una conexión a la base de datos PostgreSQL.
    Las credenciales se obtienen de variables de entorno.
    """
    db_host = os.environ.get("DB_HOST", "localhost") # Default to localhost for development
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    db_port = os.environ.get("DB_PORT", "5432") # Standard PostgreSQL port is 5432

    if not all([db_host, db_name, db_user, db_password]):
        logging.warning("ADVERTENCIA: Una o más variables de entorno de la base de datos no están configuradas. Intentando conectar con valores predeterminados o vacíos.")

    try:
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password, port=db_port)
        logging.info("Conexión a la DB establecida exitosamente.")
        return conn
    except Error as e:
        logging.error(f"Error al conectar a la base de datos PostgreSQL: {e}")
        return None

class DatabaseConnection:
    """
    Context manager para manejar la conexión a la base de datos.
    Asegura que la conexión se cierre y las transacciones se manejen correctamente (commit/rollback).
    """
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = get_db_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None: # No hubo excepción, hacer commit
                self.conn.commit()
            else: # Hubo una excepción, hacer rollback
                self.conn.rollback()
                logging.error(f"Transacción revertida debido a un error: {exc_val}")
            self.conn.close()

def crear_tablas():
    """
    Crea las tablas 'usuarios', 'facturas' y 'detalles_soat' si no existen.
    También inserta usuarios predeterminados si no existen.
    """
    conn = get_db_connection()
    if conn:
        try:
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
                        estado_auditoria TEXT DEFAULT 'Pendiente',
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
                conn.commit()
                logging.info("Tablas verificadas/creadas y usuarios predeterminados insertados.")
        except Error as e:
            logging.error(f"Error al crear tablas o insertar usuarios: {e}")
            conn.rollback()
        finally:
            conn.close()

def obtener_credenciales_usuario(username):
    """
    Obtiene la contraseña y el rol de un usuario por su nombre de usuario.
    """
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
    """
    Guarda una nueva factura en la base de datos.
    Retorna el ID de la factura insertada o None si falla (ej. duplicado).
    """
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
    """
    Guarda los detalles SOAT para una factura específica.
    """
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
    """
    Obtiene los detalles de una factura por su ID.
    """
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

def obtener_detalles_soat_por_factura_id(factura_id):
    """
    Obtiene los detalles SOAT de una factura por su ID.
    """
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
    """
    Actualiza una factura existente en la base de datos.
    Retorna True si la actualización fue exitosa, False en caso contrario.
    """
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
    """
    Actualiza el estado de auditoría, observación y tipo de error de una factura.
    """
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
    """
    Actualiza la fecha de entrega al radicador para una factura.
    """
    try:
        with DatabaseConnection() as conn:
            if conn is None: return False
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE facturas SET fecha_entrega_radicador = %s WHERE id = %s;
                """, (fecha_entrega, factura_id))
                logging.info(f"Fecha de entrega al radicador para factura ID: {factura_id} actualizada.")
                return True
    except Error as e:
        logging.error(f"Error al actualizar fecha de entrega al radicador para factura ID: {factura_id}: {e}")
        return False

def eliminar_factura(factura_id):
    """
    Elimina una factura de la base de datos.
    """
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

def guardar_factura_reemplazo(old_factura_id, new_numero_factura, new_fecha_generacion,
                              area_servicio, facturador, eps, fecha_reemplazo):
    """
    Guarda una factura de reemplazo y actualiza el estado de la factura original.
    """
    try:
        with DatabaseConnection() as conn:
            if conn is None: return False
            with conn.cursor() as cursor:
                # Insertar la nueva factura de reemplazo
                cursor.execute("""
                    INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps,
                                          fecha_hora_entrega, factura_original_id, estado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
                """, (new_numero_factura, area_servicio, facturador, new_fecha_generacion, eps,
                      datetime.now(), old_factura_id, 'Activa')) # fecha_hora_entrega se establece a la hora actual de ingreso
                new_factura_id = cursor.fetchone()[0]

                # Actualizar el estado de la factura original
                cursor.execute("""
                    UPDATE facturas SET
                        estado = 'Reemplazada', reemplazada_por_numero_factura = %s, fecha_reemplazo = %s
                    WHERE id = %s;
                """, (new_numero_factura, fecha_reemplazo, old_factura_id))
                logging.info(f"Factura ID: {old_factura_id} reemplazada por nueva factura ID: {new_factura_id} ({new_numero_factura}).")
                return True
    except errors.UniqueViolation as e:
        logging.warning(f"Intento de guardar factura de reemplazo con combinación duplicada: (Número: '{new_numero_factura}', Legalizador: '{facturador}', EPS: '{eps}', Área: '{area_servicio}')")
        return False
    except Error as e:
        logging.error(f"Error al guardar factura de reemplazo para ID original {old_factura_id}: {e}")
        return False

def cargar_facturas(search_term=None, search_column=None):
    """
    Carga facturas de la base de datos, opcionalmente filtrando por un término de búsqueda y columna.
    """
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
    """
    Obtiene el conteo de facturas pendientes agrupadas por legalizador y EPS.
    """
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

def obtener_conteo_facturas_radicadas_ok():
    """
    Obtiene el conteo total de facturas con estado 'Radicada OK'.
    """
    try:
        with DatabaseConnection() as conn:
            if conn is None: return 0
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(id) FROM facturas WHERE estado_auditoria = 'Radicada OK';")
                count = cursor.fetchone()[0]
                logging.info(f"Conteo de facturas radicadas OK: {count}")
                return count
    except Error as e:
        logging.error(f"Error al obtener conteo de facturas radicadas OK: {e}")
        return 0

def obtener_conteo_facturas_con_errores():
    """
    Obtiene el conteo total de facturas con errores (Devuelta por Auditor, Corregida por Legalizador).
    """
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
    """
    Obtiene el conteo total de facturas con estado 'Pendiente'.
    """
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

def obtener_conteo_total_facturas():
    """
    Obtiene el conteo total de todas las facturas.
    """
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
    """
    Obtiene una lista de todos los facturadores únicos registrados en las facturas.
    """
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
    """
    Obtiene una lista de todas las EPS únicas registradas en las facturas.
    """
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
