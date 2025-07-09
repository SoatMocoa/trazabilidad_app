import os
import psycopg2 # Importar la librería para PostgreSQL
from psycopg2 import Error # Importar la clase Error para manejar excepciones de psycopg2
from datetime import datetime # Necesario para guardar_factura_reemplazo
import time # Importar la librería time para depuración de tiempos

# --- Funciones de Conexión a la Base de Datos ---

def get_db_connection():
    """
    Establece y retorna una conexión a la base de datos PostgreSQL en Supabase.
    Las credenciales se obtienen de variables de entorno.
    """
    db_host = os.environ.get("DB_HOST")
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    db_port = os.environ.get("DB_PORT", "5432")

    # --- INICIO DE DEPURACIÓN (RE-HABILITADO) ---
    print("\n--- Verificando variables de entorno para la DB ---")
    print(f"DB_HOST: {db_host}")
    print(f"DB_NAME: {db_name}")
    print(f"DB_USER: {db_user}")
    print(f"DB_PASSWORD: {'*' * len(db_password) if db_password else 'None'}") # Ocultar contraseña por seguridad
    print(f"DB_PORT: {db_port}")
    print("---------------------------------------------------\n")
    # --- FIN DE DEPURACIÓN ---

    if not all([db_host, db_name, db_user, db_password]):
        print("ADVERTENCIA: Una o más variables de entorno de la base de datos no están configuradas.")
        print("Intentando conectar a localhost como fallback (esto causará un error si no hay una DB local).")

    try:
        print("DEBUG: Intentando establecer conexión con PostgreSQL...")
        start_time = time.time()
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        end_time = time.time()
        print(f"DEBUG: Conexión a la DB establecida en {end_time - start_time:.4f} segundos.")
        return conn
    except Error as e:
        print(f"Error al conectar a la base de datos PostgreSQL: {e}")
        print("DEBUG: La función get_db_connection retornó None debido a un error.")
        return None

# --- Operaciones de la Base de Datos ---

def crear_tablas():
    """
    Crea las tablas 'usuarios', 'facturas' y 'detalles_soat' en la base de datos
    si no existen. También inserta usuarios por defecto.
    """
    print("DEBUG: Llamando a get_db_connection para crear tablas.")
    conn = get_db_connection()
    if conn:
        print("DEBUG: Conexión obtenida exitosamente en crear_tablas. Procediendo a crear/verificar tablas.")
        try:
            cursor = conn.cursor()
            start_time = time.time()

            # Tabla de Usuarios
            try:
                print("DEBUG: Creando/Verificando tabla 'usuarios'...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS usuarios (
                        id SERIAL PRIMARY KEY,
                        username TEXT NOT NULL UNIQUE,
                        password TEXT NOT NULL,
                        role TEXT NOT NULL
                    );
                """)
                print("DEBUG: Tabla 'usuarios' verificada/creada.")
            except Error as e:
                print(f"ERROR: Fallo al crear/verificar tabla 'usuarios': {e}")
                raise # Re-lanzar para que el bloque except principal lo capture

            # Insertar usuarios por defecto si no existen
            try:
                print("DEBUG: Insertando usuarios por defecto...")
                cursor.execute("""
                    INSERT INTO usuarios (username, password, role) VALUES ('legalizador', 'legalizador123', 'legalizador')
                    ON CONFLICT (username) DO NOTHING;
                """)
                cursor.execute("""
                    INSERT INTO usuarios (username, password, role) VALUES ('auditor', 'auditor123', 'auditor')
                    ON CONFLICT (username) DO NOTHING;
                """)
                print("DEBUG: Usuarios por defecto insertados/verificados.")
            except Error as e:
                print(f"ERROR: Fallo al insertar usuarios por defecto: {e}")
                raise # Re-lanzar

            # Tabla de Facturas
            try:
                print("DEBUG: Creando/Verificando tabla 'facturas'...")
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
                        estado TEXT DEFAULT 'Activa', -- Activa, Anulada, Reemplazada
                        reemplazada_por_numero_factura TEXT, -- Nuevo campo para el número de la factura que la reemplazó
                        estado_auditoria TEXT DEFAULT 'Pendiente', -- Pendiente, Radicada OK, Devuelta por Auditor, Corregida por Legalizador
                        observacion_auditor TEXT,
                        tipo_error TEXT,
                        fecha_reemplazo TEXT, -- Fecha en que la factura fue marcada como reemplazada
                        fecha_entrega_radicador TEXT, -- Nueva columna para la fecha de entrega al radicador
                        FOREIGN KEY (factura_original_id) REFERENCES facturas(id)
                    );
                """)
                print("DEBUG: Tabla 'facturas' verificada/creada.")
            except Error as e:
                print(f"ERROR: Fallo al crear/verificar tabla 'facturas': {e}")
                raise # Re-lanzar

            # Tabla de Detalles SOAT
            try:
                print("DEBUG: Creando/Verificando tabla 'detalles_soat'...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS detalles_soat (
                        id SERIAL PRIMARY KEY,
                        factura_id INTEGER UNIQUE,
                        fecha_generacion_soat TEXT,
                        FOREIGN KEY (factura_id) REFERENCES facturas(id) ON DELETE CASCADE
                    );
                """)
                print("DEBUG: Tabla 'detalles_soat' verificada/creada.")
            except Error as e:
                print(f"ERROR: Fallo al crear/verificar tabla 'detalles_soat': {e}")
                raise # Re-lanzar

            conn.commit()
            end_time = time.time()
            print(f"DEBUG: Tablas verificadas/creadas y cambios confirmados en {end_time - start_time:.4f} segundos.")
        except Error as e:
            print(f"Error general durante la creación/verificación de tablas: {e}")
            if conn:
                print("DEBUG: Intentando rollback debido a un error...")
                try:
                    conn.rollback()
                    print("DEBUG: Rollback exitoso.")
                except Error as rollback_e:
                    print(f"ERROR: Fallo al realizar rollback: {rollback_e}")
        finally:
            if conn:
                print("DEBUG: Cerrando conexión a la DB...")
                conn.close()
                print("DEBUG: Conexión a la DB cerrada.")
    else:
        print("ADVERTENCIA: No se pudo obtener una conexión a la base de datos en crear_tablas. Las tablas no se crearon/verificaron.")

def obtener_credenciales_usuario(username):
    """
    Obtiene la contraseña y el rol de un usuario de la base de datos.
    Retorna una tupla (password, role) o None si el usuario no existe.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT password, role FROM usuarios WHERE username = %s;", (username,))
            user_data = cursor.fetchone()
            return user_data
        except Error as e:
            print(f"Error al obtener credenciales del usuario: {e}")
            return None
        finally:
            if conn:
                conn.close()
    return None

def guardar_factura(facturador, eps, numero_factura, fecha_generacion, area_servicio, fecha_hora_entrega):
    """
    Guarda una nueva factura en la base de datos.
    Retorna el ID de la factura insertada o None si ya existe.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM facturas WHERE numero_factura = %s;", (numero_factura,))
            if cursor.fetchone():
                print(f"Factura con número {numero_factura} ya existe. No se insertó.")
                return None

            cursor.execute("""
                INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
            """, (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega))
            factura_id = cursor.fetchone()[0]
            conn.commit()
            return factura_id
        except Error as e:
            print(f"Error al guardar factura: {e}")
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                conn.close()
    return None

def insertar_factura_bulk(numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega):
    """
    Inserta una factura en la base de datos para carga masiva.
    Retorna el ID de la factura insertada o None si ya existe.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM facturas WHERE numero_factura = %s;", (numero_factura,))
            if cursor.fetchone():
                return None

            cursor.execute("""
                INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
            """, (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega))
            factura_id = cursor.fetchone()[0]
            conn.commit()
            return factura_id
        except Error as e:
            print(f"Error al insertar factura en bulk: {e}")
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                conn.close()
    return None

def guardar_detalles_soat(factura_id, fecha_generacion_soat):
    """
    Guarda los detalles SOAT para una factura específica.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO detalles_soat (factura_id, fecha_generacion_soat)
                VALUES (%s, %s);
            """, (factura_id, fecha_generacion_soat))
            conn.commit()
            return True
        except Error as e:
            print(f"Error al guardar detalles SOAT: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    return False

def insertar_detalles_soat_bulk(factura_id, fecha_generacion_soat):
    """
    Inserta detalles SOAT en la base de datos para carga masiva.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO detalles_soat (factura_id, fecha_generacion_soat)
                VALUES (%s, %s);
            """, (factura_id, fecha_generacion_soat))
            conn.commit()
            return True
        except Error as e:
            print(f"Error al insertar detalles SOAT en bulk: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    return False

def obtener_factura_por_id(factura_id):
    """
    Obtiene los datos completos de una factura por su ID,
    incluyendo información de la factura original si es un reemplazo.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    f.id, f.numero_factura, f.area_servicio, f.facturador, f.fecha_generacion, f.eps,
                    f.fecha_hora_entrega, f.tiene_correccion, f.descripcion_devolucion,
                    f.fecha_devolucion_lider, f.revisado, f.factura_original_id, f.estado,
                    f.reemplazada_por_numero_factura, f.estado_auditoria, f.observacion_auditor,
                    f.tipo_error, f.fecha_reemplazo,
                    fo.numero_factura AS num_fact_original_linked,
                    fo.fecha_generacion AS fecha_gen_original_linked,
                    f.fecha_entrega_radicador
                FROM
                    facturas f
                LEFT JOIN
                    facturas fo ON f.factura_original_id = fo.id
                WHERE
                    f.id = %s;
            """, (factura_id,))
            factura_data = cursor.fetchone()
            return factura_data
        except Error as e:
            print(f"Error al obtener factura por ID: {e}")
            return None
        finally:
            if conn:
                conn.close()
    return None

def actualizar_factura(factura_id, numero_factura, area_servicio, facturador, fecha_generacion, eps,
                       fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
                       fecha_devolucion_lider, revisado, factura_original_id, estado,
                       reemplazada_por_numero_factura, estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo):
    """
    Actualiza una factura existente en la base de datos.
    Retorna True si la actualización fue exitosa, False en caso contrario.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM facturas WHERE numero_factura = %s AND id != %s;", (numero_factura, factura_id))
            if cursor.fetchone():
                print(f"Error: El numero de factura '{numero_factura}' ya existe para otra factura.")
                return False

            cursor.execute("""
                UPDATE facturas SET
                    numero_factura = %s,
                    area_servicio = %s,
                    facturador = %s,
                    fecha_generacion = %s,
                    eps = %s,
                    fecha_hora_entrega = %s,
                    tiene_correccion = %s,
                    descripcion_devolucion = %s,
                    fecha_devolucion_lider = %s,
                    revisado = %s,
                    factura_original_id = %s,
                    estado = %s,
                    reemplazada_por_numero_factura = %s,
                    estado_auditoria = %s,
                    observacion_auditor = %s,
                    tipo_error = %s,
                    fecha_reemplazo = %s
                WHERE id = %s;
            """, (numero_factura, area_servicio, facturador, fecha_generacion, eps,
                  fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
                  fecha_devolucion_lider, revisado, factura_original_id, estado,
                  reemplazada_por_numero_factura, estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo,
                  factura_id))
            conn.commit()
            return True
        except Error as e:
            print(f"Error al actualizar factura: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    return False

def actualizar_estado_auditoria_factura(factura_id, nuevo_estado_auditoria, observacion, tipo_error):
    """
    Actualiza el estado de auditoría, observación y tipo de error de una factura.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE facturas SET
                    estado_auditoria = %s,
                    observacion_auditor = %s,
                    tipo_error = %s
                WHERE id = %s;
            """, (nuevo_estado_auditoria, observacion, tipo_error, factura_id))
            conn.commit()
            return True
        except Error as e:
            print(f"Error al actualizar estado de auditoria: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    return False

def actualizar_fecha_entrega_radicador(factura_id, fecha_entrega):
    """
    Actualiza la fecha_entrega_radicador de una factura.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE facturas SET
                    fecha_entrega_radicador = %s
                WHERE id = %s;
            """, (fecha_entrega, factura_id))
            conn.commit()
            return True
        except Error as e:
            print(f"Error al actualizar fecha de entrega al radicador: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    return False

def eliminar_factura(factura_id):
    """
    Elimina una factura de la base de datos.
    Retorna True si la eliminación fue exitosa, False en caso contrario.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM facturas WHERE id = %s;", (factura_id,))
            conn.commit()
            return True
        except Error as e:
            print(f"Error al eliminar factura: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    return False

def guardar_factura_reemplazo(old_factura_id, new_numero_factura, new_fecha_generacion,
                              area_servicio, facturador, eps, fecha_reemplazo):
    """
    Guarda una nueva factura como reemplazo de una existente y actualiza la factura original.
    Retorna True si la operación fue exitosa, False en caso contrario.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # 1. Verificar si el nuevo numero_factura ya existe
            cursor.execute("SELECT id FROM facturas WHERE numero_factura = %s;", (new_numero_factura,))
            if cursor.fetchone():
                print(f"Error: El numero de factura de reemplazo '{new_numero_factura}' ya existe.")
                return False

            # 2. Insertar la nueva factura de reemplazo
            cursor.execute("""
                INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps,
                                      fecha_hora_entrega, factura_original_id, estado)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
            """, (new_numero_factura, area_servicio, facturador, new_fecha_generacion, eps,
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'), old_factura_id, 'Activa')) # La nueva factura está 'Activa'
            new_factura_id = cursor.fetchone()[0]

            # 3. Actualizar la factura original para marcarla como 'Reemplazada'
            cursor.execute("""
                UPDATE facturas SET
                    estado = 'Reemplazada',
                    reemplazada_por_numero_factura = %s,
                    fecha_reemplazo = %s
                WHERE id = %s;
            """, (new_numero_factura, fecha_reemplazo, old_factura_id))

            conn.commit()
            return True
        except Error as e:
            print(f"Error al guardar factura de reemplazo: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    return False

def cargar_facturas(search_term=None, search_column=None):
    """
    Carga todas las facturas de la base de datos, con opción de filtro.
    Incluye un JOIN para obtener el número y fecha de generación de la factura original
    cuando la factura actual es un reemplazo.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            start_time = time.time()
            query = """
                SELECT
                    f.id, f.numero_factura, f.area_servicio, f.facturador, f.fecha_generacion, f.eps,
                    f.fecha_hora_entrega, f.tiene_correccion, f.descripcion_devolucion,
                    f.fecha_devolucion_lider, f.revisado, f.factura_original_id, f.estado,
                    f.reemplazada_por_numero_factura, f.estado_auditoria, f.observacion_auditor,
                    f.tipo_error, f.fecha_reemplazo,
                    fo.numero_factura AS num_fact_original_linked,
                    fo.fecha_generacion AS fecha_gen_original_linked,
                    f.fecha_entrega_radicador
                FROM
                    facturas f
                LEFT JOIN
                    facturas fo ON f.factura_original_id = fo.id
            """
            params = []
            if search_term and search_column:
                # Asegurarse de que search_term no tenga espacios en blanco al inicio/final
                clean_search_term = search_term.strip()
                # Usar ILIKE para búsqueda insensible a mayúsculas/minúsculas en PostgreSQL
                query += f" WHERE {search_column} ILIKE %s"
                params.append(f"%{clean_search_term}%")
            
            # Ordenar por id descendente para ver las más recientes primero
            query += " ORDER BY f.id DESC;"

            # --- DEBUGGING: Imprimir la consulta SQL y los parámetros ---
            print(f"\nDEBUG (DB - FILTRO): Ejecutando consulta: {query}")
            print(f"DEBUG (DB - FILTRO): Con parámetros: {params}\n")

            # Para depuración visual SOLAMENTE: Imprimir la consulta con parámetros sustituidos
            # NO USAR ESTO EN PRODUCCIÓN CON ENTRADAS DE USUARIO DIRECTAS POR RIESGO DE INYECCIÓN SQL
            debug_query_with_params = query
            if params:
                for p in params:
                    # Reemplazar %s con el parámetro real, manejando cadenas con comillas
                    if isinstance(p, str):
                        # Escapar comillas simples dentro de la cadena para la visualización
                        escaped_p = p.replace("'", "''")
                        debug_query_with_params = debug_query_with_params.replace("%s", f"'{escaped_p}'", 1)
                    else:
                        debug_query_with_params = debug_query_with_params.replace("%s", str(p), 1)
            print(f"DEBUG (DB - FILTRO): Consulta con parámetros (solo para depuración visual): {debug_query_with_params}\n")
            # --- FIN DEBUGGING ---

            cursor.execute(query, tuple(params))
            facturas = cursor.fetchall()
            end_time = time.time()
            return facturas
        except Error as e:
            print(f"Error al cargar facturas: {e}")
            return []
        finally:
            if conn:
                conn.close()
    return []

def obtener_conteo_facturas_por_legalizador_y_eps():
    """
    Obtiene el conteo de facturas pendientes por cada legalizador y EPS.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    facturador,
                    eps,
                    COUNT(id)
                FROM
                    facturas
                WHERE
                    estado_auditoria = 'Pendiente'
                GROUP BY
                    facturador, eps
                ORDER BY
                    facturador, eps;
            """)
            stats = cursor.fetchall()
            return stats
        except Error as e:
            print(f"Error al obtener estadísticas de facturas pendientes: {e}")
            return []
        finally:
            if conn:
                conn.close()
    return []

# Alias para la función de estadísticas para mantener la compatibilidad con el frontend
obtener_conteo_facturas_pendientes_por_legalizador_y_eps = obtener_conteo_facturas_por_legalizador_y_eps

def obtener_conteo_facturas_radicadas_ok():
    """
    Obtiene el conteo de facturas con estado_auditoria 'Radicada OK'.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    COUNT(id)
                FROM
                    facturas
                WHERE
                    estado_auditoria = 'Radicada OK';
            """)
            count = cursor.fetchone()[0]
            return count
        except Error as e:
            print(f"Error al obtener conteo de facturas radicadas OK: {e}")
            return 0
        finally:
            if conn:
                conn.close()
    return 0

def obtener_conteo_facturas_con_errores():
    """
    Obtiene el conteo de facturas con estado_auditoria 'Devuelta por Auditor' o 'Corregida por Legalizador'.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    COUNT(id)
                FROM
                    facturas
                WHERE
                    estado_auditoria IN ('Devuelta por Auditor', 'Corregida por Legalizador');
            """)
            count = cursor.fetchone()[0]
            return count
        except Error as e:
            print(f"Error al obtener conteo de facturas con errores: {e}")
            return 0
        finally:
            if conn:
                conn.close()
    return 0

def obtener_conteo_facturas_pendientes_global():
    """
    Obtiene el conteo total de facturas con estado_auditoria 'Pendiente'.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    COUNT(id)
                FROM
                    facturas
                WHERE
                    estado_auditoria = 'Pendiente';
            """)
            count = cursor.fetchone()[0]
            return count
        except Error as e:
            print(f"Error al obtener conteo total de facturas pendientes: {e}")
            return 0
        finally:
            if conn:
                conn.close()
    return 0

def obtener_conteo_total_facturas():
    """
    Obtiene el conteo total de todas las facturas en el sistema.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    COUNT(id)
                FROM
                    facturas;
            """)
            count = cursor.fetchone()[0]
            return count
        except Error as e:
            print(f"Error al obtener conteo total de facturas: {e}")
            return 0
        finally:
            if conn:
                conn.close()
    return 0
