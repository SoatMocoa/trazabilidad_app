import os
import psycopg2
from psycopg2 import Error
from datetime import datetime
import time

def get_db_connection():
    db_host = os.environ.get("DB_HOST")
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    db_port = os.environ.get("DB_PORT", "6543")

    print("\n--- Verificando variables de entorno para la DB ---")
    print(f"DB_HOST: {db_host}")
    print(f"DB_NAME: {db_name}")
    print(f"DB_USER: {db_user}")
    print(f"DB_PASSWORD: {'*' * len(db_password) if db_password else 'None'}")
    print(f"DB_PORT: {db_port}")
    print("---------------------------------------------------\n")

    if not all([db_host, db_name, db_user, db_password]):
        print("ADVERTENCIA: Una o más variables de entorno de la base de datos no están configuradas.")
        print("Intentando conectar a localhost como fallback")

    try:
        start_time = time.time()
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password, port=db_port)
        end_time = time.time()
        print(f"DEBUG: Conexión a la DB establecida en {end_time - start_time:.4f} segundos.")
        return conn
    except Error as e:
        print(f"Error al conectar a la base de datos PostgreSQL: {e}")
        return None

def crear_tablas():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    id SERIAL PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    password TEXT NOT NULL,
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
                    numero_factura TEXT NOT NULL UNIQUE,
                    area_servicio TEXT,
                    facturador TEXT,
                    fecha_generacion TEXT,
                    eps TEXT,
                    fecha_hora_entrega TEXT,
                    tiene_correccion BOOLEAN DEFAULT FALSE,
                    descripcion_devolucion TEXT,
                    fecha_devolucion_lider TEXT,
                    revisado BOOLEAN DEFAULT FALSE,
                    factura_original_id INTEGER,
                    estado TEXT DEFAULT 'Activa',
                    reemplazada_por_numero_factura TEXT,
                    estado_auditoria TEXT DEFAULT 'Pendiente',
                    observacion_auditor TEXT,
                    tipo_error TEXT,
                    fecha_reemplazo TEXT,
                    fecha_entrega_radicador TEXT,
                    FOREIGN KEY (factura_original_id) REFERENCES facturas(id)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS detalles_soat (
                    id SERIAL PRIMARY KEY,
                    factura_id INTEGER UNIQUE,
                    fecha_generacion_soat TEXT,
                    FOREIGN KEY (factura_id) REFERENCES facturas(id) ON DELETE CASCADE
                );
            """)
            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Tablas verificadas/creadas en {end_time - start_time:.4f} segundos.")
        except Error as e:
            print(f"Error al crear tablas o insertar usuarios: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

def obtener_credenciales_usuario(username):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT password, role FROM usuarios WHERE username = %s;", (username,))
            user_data = cursor.fetchone()
            end_time = time.time()
            print(f"DEBUG: Obtener credenciales de usuario en {end_time - start_time:.4f} segundos.")
            return user_data
        except Error as e:
            print(f"Error al obtener credenciales del usuario: {e}")
            return None
        finally:
            if conn: conn.close()
    return None

def guardar_factura(facturador, eps, numero_factura, fecha_generacion, area_servicio, fecha_hora_entrega):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT id FROM facturas WHERE numero_factura = %s;", (numero_factura,))
            if cursor.fetchone(): return None
            cursor.execute("""
                INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
            """, (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega))
            factura_id = cursor.fetchone()[0]
            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Guardar factura en {end_time - start_time:.4f} segundos.")
            return factura_id
        except Error as e:
            print(f"Error al guardar factura: {e}")
            if conn: conn.rollback()
            return None
        finally:
            if conn: conn.close()
    return None

def insertar_factura_bulk(numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT id FROM facturas WHERE numero_factura = %s;", (numero_factura,))
            if cursor.fetchone(): return None
            cursor.execute("""
                INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
            """, (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega))
            factura_id = cursor.fetchone()[0]
            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Insertar factura bulk en {end_time - start_time:.4f} segundos.")
            return factura_id
        except Error as e:
            print(f"Error al insertar factura en bulk: {e}")
            if conn: conn.rollback()
            return None
        finally:
            if conn: conn.close()
    return None

def guardar_detalles_soat(factura_id, fecha_generacion_soat):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("""
                INSERT INTO detalles_soat (factura_id, fecha_generacion_soat)
                VALUES (%s, %s);
            """, (factura_id, fecha_generacion_soat))
            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Guardar detalles SOAT en {end_time - start_time:.4f} segundos.")
            return True
        except Error as e:
            print(f"Error al guardar detalles SOAT: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    return False

def insertar_detalles_soat_bulk(factura_id, fecha_generacion_soat):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("""
                INSERT INTO detalles_soat (factura_id, fecha_generacion_soat)
                VALUES (%s, %s);
            """, (factura_id, fecha_generacion_soat))
            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Insertar detalles SOAT bulk en {end_time - start_time:.4f} segundos.")
            return True
        except Error as e:
            print(f"Error al insertar detalles SOAT en bulk: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    return None

def obtener_factura_por_id(factura_id):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("""
                SELECT f.id, f.numero_factura, f.area_servicio, f.facturador, f.fecha_generacion, f.eps,
                    f.fecha_hora_entrega, f.tiene_correccion, f.descripcion_devolucion,
                    f.fecha_devolucion_lider, f.revisado, f.factura_original_id, f.estado,
                    f.reemplazada_por_numero_factura, f.estado_auditoria, f.observacion_auditor,
                    f.tipo_error, f.fecha_reemplazo,
                    fo.numero_factura AS num_fact_original_linked,
                    fo.fecha_generacion AS fecha_gen_original_linked,
                    f.fecha_entrega_radicador
                FROM facturas f
                LEFT JOIN facturas fo ON f.factura_original_id = fo.id
                WHERE f.id = %s;
            """, (factura_id,))
            factura_data = cursor.fetchone()
            end_time = time.time()
            print(f"DEBUG: Obtener factura por ID en {end_time - start_time:.4f} segundos.")
            return factura_data
        except Error as e:
            print(f"Error al obtener factura por ID: {e}")
            return None
        finally:
            if conn: conn.close()
    return None

def obtener_detalles_soat_por_factura_id(factura_id):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("""
                SELECT id, factura_id, fecha_generacion_soat
                FROM detalles_soat
                WHERE factura_id = %s;
            """, (factura_id,))
            soat_details = cursor.fetchone()
            end_time = time.time()
            print(f"DEBUG: Obtener detalles SOAT por factura ID en {end_time - start_time:.4f} segundos.")
            return soat_details
        except Error as e:
            print(f"Error al obtener detalles SOAT por factura ID: {e}")
            return None
        finally:
            if conn: conn.close()
    return None

def actualizar_factura(factura_id, numero_factura, area_servicio, facturador, fecha_generacion, eps,
                         fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
                         fecha_devolucion_lider, revisado, factura_original_id, estado,
                         reemplazada_por_numero_factura, estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT id FROM facturas WHERE numero_factura = %s AND id != %s;", (numero_factura, factura_id))
            if cursor.fetchone(): return False
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
            end_time = time.time()
            print(f"DEBUG: Actualizar factura en {end_time - start_time:.4f} segundos.")
            return True
        except Error as e:
            print(f"Error al actualizar factura: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    return False

def actualizar_estado_auditoria_factura(factura_id, nuevo_estado_auditoria, observacion, tipo_error):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("""
                UPDATE facturas SET
                    estado_auditoria = %s, observacion_auditor = %s, tipo_error = %s
                WHERE id = %s;
            """, (nuevo_estado_auditoria, observacion, tipo_error, factura_id))
            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Actualizar estado auditoria en {end_time - start_time:.4f} segundos.")
            return True
        except Error as e:
            print(f"Error al actualizar estado de auditoria: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    return False

def actualizar_fecha_entrega_radicador(factura_id, fecha_entrega):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("""
                UPDATE facturas SET fecha_entrega_radicador = %s WHERE id = %s;
            """, (fecha_entrega, factura_id))
            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Actualizar fecha entrega radicador en {end_time - start_time:.4f} segundos.")
            return True
        except Error as e:
            print(f"Error al actualizar fecha de entrega al radicador: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    return False

def eliminar_factura(factura_id):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("DELETE FROM facturas WHERE id = %s;", (factura_id,))
            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Eliminar factura en {end_time - start_time:.4f} segundos.")
            return True
        except Error as e:
            print(f"Error al eliminar factura: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    return False

def guardar_factura_reemplazo(old_factura_id, new_numero_factura, new_fecha_generacion,
                              area_servicio, facturador, eps, fecha_reemplazo):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT id FROM facturas WHERE numero_factura = %s;", (new_numero_factura,))
            if cursor.fetchone(): return False
            cursor.execute("""
                INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps,
                                      fecha_hora_entrega, factura_original_id, estado)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
            """, (new_numero_factura, area_servicio, facturador, new_fecha_generacion, eps,
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'), old_factura_id, 'Activa'))
            new_factura_id = cursor.fetchone()[0]
            cursor.execute("""
                UPDATE facturas SET
                    estado = 'Reemplazada', reemplazada_por_numero_factura = %s, fecha_reemplazo = %s
                WHERE id = %s;
            """, (new_numero_factura, fecha_reemplazo, old_factura_id))
            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Guardar factura reemplazo en {end_time - start_time:.4f} segundos.")
            return True
        except Error as e:
            print(f"Error al guardar factura de reemplazo: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    return False

def cargar_facturas(search_term=None, search_column=None):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            query = """
                SELECT f.id, f.numero_factura, f.area_servicio, f.facturador, f.fecha_generacion, f.eps,
                    f.fecha_hora_entrega, f.tiene_correccion, f.descripcion_devolucion,
                    f.fecha_devolucion_lider, f.revisado, f.factura_original_id, f.estado,
                    f.reemplazada_por_numero_factura, f.estado_auditoria, f.observacion_auditor,
                    f.tipo_error, f.fecha_reemplazo,
                    fo.numero_factura AS num_fact_original_linked,
                    fo.fecha_generacion AS fecha_gen_original_linked,
                    f.fecha_entrega_radicador
                FROM facturas f
                LEFT JOIN facturas fo ON f.factura_original_id = fo.id
            """
            params = []
            if search_term and search_column:
                query += f" WHERE f.{search_column} ILIKE %s"
                params.append(f"%{search_term}%")
            query += " ORDER BY f.id DESC;"
            cursor.execute(query, tuple(params))
            facturas = cursor.fetchall()
            end_time = time.time()
            print(f"DEBUG: Cargar facturas en {end_time - start_time:.4f} segundos.")
            return facturas
        except Error as e:
            print(f"Error al cargar facturas: {e}")
            return []
        finally:
            if conn: conn.close()
    return []

def obtener_conteo_facturas_por_legalizador_y_eps():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("""
                SELECT facturador, eps, COUNT(id)
                FROM facturas
                WHERE estado_auditoria = 'Pendiente'
                GROUP BY facturador, eps
                ORDER BY facturador, eps;
            """)
            stats = cursor.fetchall()
            end_time = time.time()
            print(f"DEBUG: Obtener conteo facturas pendientes en {end_time - start_time:.4f} segundos.")
            return stats
        except Error as e:
            print(f"Error al obtener estadísticas de facturas pendientes: {e}")
            return []
        finally:
            if conn: conn.close()
    return []

obtener_conteo_facturas_pendientes_por_legalizador_y_eps = obtener_conteo_facturas_por_legalizador_y_eps

def obtener_conteo_facturas_radicadas_ok():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT COUNT(id) FROM facturas WHERE estado_auditoria = 'Radicada OK';")
            count = cursor.fetchone()[0]
            end_time = time.time()
            print(f"DEBUG: Obtener conteo facturas radicadas OK en {end_time - start_time:.4f} segundos.")
            return count
        except Error as e:
            print(f"Error al obtener conteo de facturas radicadas OK: {e}")
            return 0
        finally:
            if conn: conn.close()
    return 0

def obtener_conteo_facturas_con_errores():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("""
                SELECT COUNT(id) FROM facturas
                WHERE estado_auditoria IN ('Devuelta por Auditor', 'Corregida por Legalizador');
            """)
            count = cursor.fetchone()[0]
            end_time = time.time()
            print(f"DEBUG: Obtener conteo facturas con errores en {end_time - start_time:.4f} segundos.")
            return count
        except Error as e:
            print(f"Error al obtener conteo de facturas con errores: {e}")
            return 0
        finally:
            if conn: conn.close()
    return 0

def obtener_conteo_facturas_pendientes_global():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT COUNT(id) FROM facturas WHERE estado_auditoria = 'Pendiente';")
            count = cursor.fetchone()[0]
            end_time = time.time()
            print(f"DEBUG: Obtener conteo facturas pendientes global en {end_time - start_time:.4f} segundos.")
            return count
        except Error as e:
            print(f"Error al obtener conteo total de facturas pendientes: {e}")
            return 0
        finally:
            if conn: conn.close()
    return 0

def obtener_conteo_total_facturas():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT COUNT(id) FROM facturas;")
            count = cursor.fetchone()[0]
            end_time = time.time()
            print(f"DEBUG: Obtener conteo total de facturas en {end_time - start_time:.4f} segundos.")
            return count
        except Error as e:
            print(f"Error al obtener conteo total de facturas: {e}")
            return 0
        finally:
            if conn: conn.close()
    return 0

def obtener_facturadores_unicos():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT DISTINCT facturador FROM facturas WHERE facturador IS NOT NULL ORDER BY facturador;")
            facturadores = [row[0] for row in cursor.fetchall()]
            end_time = time.time()
            print(f"DEBUG: Obtener facturadores unicos en {end_time - start_time:.4f} segundos.")
            return facturadores
        except Error as e:
            print(f"Error al obtener facturadores únicos: {e}")
            return []
        finally:
            if conn: conn.close()
    return []

def obtener_eps_unicas():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT DISTINCT eps FROM facturas WHERE eps IS NOT NULL ORDER BY eps;")
            epss = [row[0] for row in cursor.fetchall()]
            end_time = time.time()
            print(f"DEBUG: Obtener EPS unicas en {end_time - start_time:.4f} segundos.")
            return epss
        except Error as e:
            print(f"Error al obtener EPS únicas: {e}")
            return []
        finally:
            if conn: conn.close()
    return []
