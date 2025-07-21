import os
import psycopg2
from psycopg2 import Error
from psycopg2 import errors # Importar errores específicos de psycopg2
from datetime import datetime
import logging

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
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

def crear_tablas():
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
                        numero_factura TEXT NOT NULL, -- Eliminada la restricción UNIQUE aquí para permitir la compuesta
                        area_servicio TEXT,
                        facturador TEXT,
                        fecha_generacion DATE, -- Cambiado a DATE
                        eps TEXT,
                        fecha_hora_entrega TIMESTAMP, -- Cambiado a TIMESTAMP
                        tiene_correccion BOOLEAN DEFAULT FALSE,
                        descripcion_devolucion TEXT,
                        fecha_devolucion_lider DATE, -- Cambiado a DATE
                        revisado BOOLEAN DEFAULT FALSE,
                        factura_original_id INTEGER,
                        estado TEXT DEFAULT 'Activa',
                        reemplazada_por_numero_factura TEXT,
                        estado_auditoria TEXT DEFAULT 'Pendiente',
                        observacion_auditor TEXT,
                        tipo_error TEXT,
                        fecha_reemplazo DATE, -- Cambiado a DATE
                        fecha_entrega_radicador TIMESTAMP, -- Cambiado a TIMESTAMP
                        FOREIGN KEY (factura_original_id) REFERENCES facturas(id),
                        CONSTRAINT unique_factura_details UNIQUE (numero_factura, facturador, eps, area_servicio)
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS detalles_soat (
                        id SERIAL PRIMARY KEY,
                        factura_id INTEGER UNIQUE,
                        fecha_generacion_soat DATE, -- Cambiado a DATE
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
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT password, role FROM usuarios WHERE username = %s;", (username,))
                user_data = cursor.fetchone()
                logging.info(f"Credenciales de usuario obtenidas para '{username}'.")
                return user_data
        except Error as e:
            logging.error(f"Error al obtener credenciales del usuario '{username}': {e}")
            return None
        finally:
            conn.close()
    return None

def guardar_factura(numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                # Se elimina la verificación SELECT explícita y se confía en la restricción UNIQUE de la DB.
                cursor.execute("""
                    INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
                """, (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega))
                factura_id = cursor.fetchone()[0]
                conn.commit()
                logging.info(f"Factura '{numero_factura}' guardada con ID: {factura_id}")
                return factura_id
        except errors.UniqueViolation as e:
            conn.rollback()
            # Mensaje más específico para la violación de la restricción compuesta
            logging.warning(f"Intento de guardar factura duplicada. La combinación (Número de Factura: '{numero_factura}', Legalizador: '{facturador}', EPS: '{eps}', Área de Servicio: '{area_servicio}') ya existe.")
            return None
        except Error as e:
            logging.error(f"Error al guardar factura '{numero_factura}': {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    return None

def guardar_detalles_soat(factura_id, fecha_generacion_soat):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO detalles_soat (factura_id, fecha_generacion_soat)
                    VALUES (%s, %s);
                """, (factura_id, fecha_generacion_soat))
                conn.commit()
                logging.info(f"Detalles SOAT guardados para factura ID: {factura_id}")
                return True
        except Error as e:
            logging.error(f"Error al guardar detalles SOAT para factura ID: {factura_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    return False

def obtener_factura_por_id(factura_id):
    conn = get_db_connection()
    if conn:
        try:
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
                
                # Obtener los nombres de las columnas para crear un diccionario
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
        finally:
            conn.close()
    return None

def obtener_detalles_soat_por_factura_id(factura_id):
    conn = get_db_connection()
    if conn:
        try:
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
        finally:
            conn.close()
    return None

def actualizar_factura(factura_id, numero_factura, area_servicio, facturador, fecha_generacion, eps,
                       fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
                       fecha_devolucion_lider, revisado, factura_original_id, estado,
                       reemplazada_por_numero_factura, estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                # Verificar si el número de factura ya existe para otra factura (excluyendo la actual)
                # Ahora también se verifica la unicidad compuesta para actualizaciones
                cursor.execute("""
                    SELECT id FROM facturas 
                    WHERE numero_factura = %s 
                    AND facturador = %s 
                    AND eps = %s 
                    AND area_servicio = %s 
                    AND id != %s;
                """, (numero_factura, facturador, eps, area_servicio, factura_id))
                if cursor.fetchone():
                    logging.warning(f"Intento de actualizar factura con combinación duplicada: (Número: '{numero_factura}', Legalizador: '{facturador}', EPS: '{eps}', Área: '{area_servicio}')")
                    return False

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
                conn.commit()
                logging.info(f"Factura ID: {factura_id} actualizada correctamente.")
                return True
        except Error as e:
            logging.error(f"Error al actualizar factura ID: {factura_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    return False

def actualizar_estado_auditoria_factura(factura_id, nuevo_estado_auditoria, observacion, tipo_error):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE facturas SET
                        estado_auditoria = %s, observacion_auditor = %s, tipo_error = %s
                    WHERE id = %s;
                """, (nuevo_estado_auditoria, observacion, tipo_error, factura_id))
                conn.commit()
                logging.info(f"Estado de auditoría de factura ID: {factura_id} actualizado a '{nuevo_estado_auditoria}'.")
                return True
        except Error as e:
            logging.error(f"Error al actualizar estado de auditoría de factura ID: {factura_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    return False

def actualizar_fecha_entrega_radicador(factura_id, fecha_entrega):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE facturas SET fecha_entrega_radicador = %s WHERE id = %s;
                """, (fecha_entrega, factura_id))
                conn.commit()
                logging.info(f"Fecha de entrega al radicador para factura ID: {factura_id} actualizada.")
                return True
        except Error as e:
            logging.error(f"Error al actualizar fecha de entrega al radicador para factura ID: {factura_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    return False

def eliminar_factura(factura_id):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM facturas WHERE id = %s;", (factura_id,))
                conn.commit()
                logging.info(f"Factura ID: {factura_id} eliminada correctamente.")
                return True
        except Error as e:
            logging.error(f"Error al eliminar factura ID: {factura_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    return False

def guardar_factura_reemplazo(old_factura_id, new_numero_factura, new_fecha_generacion,
                              area_servicio, facturador, eps, fecha_reemplazo):
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                # Verificar si el nuevo número de factura ya existe con la misma combinación de campos
                cursor.execute("""
                    SELECT id FROM facturas 
                    WHERE numero_factura = %s 
                    AND facturador = %s 
                    AND eps = %s 
                    AND area_servicio = %s;
                """, (new_numero_factura, facturador, eps, area_servicio))
                if cursor.fetchone():
                    logging.warning(f"Intento de guardar factura de reemplazo con combinación duplicada: (Número: '{new_numero_factura}', Legalizador: '{facturador}', EPS: '{eps}', Área: '{area_servicio}')")
                    return False

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
                conn.commit()
                logging.info(f"Factura ID: {old_factura_id} reemplazada por nueva factura ID: {new_factura_id} ({new_numero_factura}).")
                return True
        except Error as e:
            logging.error(f"Error al guardar factura de reemplazo para ID original {old_factura_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    return False

def cargar_facturas(search_term=None, search_column=None):
    conn = get_db_connection()
    if conn:
        try:
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
                
                # Obtener los nombres de las columnas
                column_names = [desc[0] for desc in cursor.description]
                
                # Obtener todas las filas y convertirlas a una lista de diccionarios
                facturas_raw = cursor.fetchall()
                facturas = [dict(zip(column_names, row)) for row in facturas_raw]
                
                logging.info(f"Cargadas {len(facturas)} facturas.")
                return facturas
        except Error as e:
            logging.error(f"Error al cargar facturas: {e}")
            return []
        finally:
            conn.close()
    return []

def obtener_conteo_facturas_por_legalizador_y_eps():
    conn = get_db_connection()
    if conn:
        try:
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
        finally:
            conn.close()
    return []

def obtener_conteo_facturas_radicadas_ok():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(id) FROM facturas WHERE estado_auditoria = 'Radicada OK';")
                count = cursor.fetchone()[0]
                logging.info(f"Conteo de facturas radicadas OK: {count}")
                return count
        except Error as e:
            logging.error(f"Error al obtener conteo de facturas radicadas OK: {e}")
            return 0
        finally:
            conn.close()
    return 0

def obtener_conteo_facturas_con_errores():
    conn = get_db_connection()
    if conn:
        try:
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
        finally:
            conn.close()
    return 0

def obtener_conteo_facturas_pendientes_global():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(id) FROM facturas WHERE estado_auditoria = 'Pendiente';")
                count = cursor.fetchone()[0]
                logging.info(f"Conteo total de facturas pendientes: {count}")
                return count
        except Error as e:
            logging.error(f"Error al obtener conteo total de facturas pendientes: {e}")
            return 0
        finally:
            conn.close()
    return 0

def obtener_conteo_total_facturas():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(id) FROM facturas;")
                count = cursor.fetchone()[0]
                logging.info(f"Conteo total de facturas: {count}")
                return count
        except Error as e:
            logging.error(f"Error al obtener conteo total de facturas: {e}")
            return 0
        finally:
            conn.close()
    return 0

def obtener_facturadores_unicos():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT facturador FROM facturas WHERE facturador IS NOT NULL ORDER BY facturador;")
                facturadores = [row[0] for row in cursor.fetchall()]
                logging.info("Facturadores únicos obtenidos.")
                return facturadores
        except Error as e:
            logging.error(f"Error al obtener facturadores únicos: {e}")
            return []
        finally:
            conn.close()
    return []

def obtener_eps_unicas():
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT eps FROM facturas WHERE eps IS NOT NULL ORDER BY eps;")
                epss = [row[0] for row in cursor.fetchall()]
                logging.info("EPS únicas obtenidas.")
                return epss
        except Error as e:
            logging.error(f"Error al obtener EPS únicas: {e}")
            return []
        finally:
            conn.close()
    return []
