import sqlite3
import os
from datetime import datetime

# Database path
DB_PATH = 'data/trazabilidad.db'

def get_db_connection():
    """Establece y retorna una conexión a la base de datos."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row # Permite acceder a las columnas por nombre
    return conn

def crear_tablas():
    """Crea las tablas de la base de datos si no existen."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Tabla de usuarios
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL CHECK (role IN ('legalizador', 'auditor'))
        );
    ''')

    # Insertar usuarios por defecto si no existen
    # Usamos INSERT OR IGNORE para evitar errores si los usuarios ya existen
    cursor.execute("INSERT OR IGNORE INTO usuarios (username, password, role) VALUES (?, ?, ?)",
                   ('legalizador', 'legalizador123', 'legalizador'))
    cursor.execute("INSERT OR IGNORE INTO usuarios (username, password, role) VALUES (?, ?, ?)",
                   ('auditor', 'auditor123', 'auditor'))


    # Tabla de facturas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_factura TEXT UNIQUE NOT NULL,
            area_servicio TEXT NOT NULL,
            facturador TEXT NOT NULL,
            fecha_generacion TEXT NOT NULL, --YYYY-MM-DD
            eps TEXT NOT NULL,
            fecha_hora_entrega TEXT, --YYYY-MM-DD HH:MM:SS
            tiene_correccion INTEGER DEFAULT 0, -- 0 para no, 1 para si
            descripcion_devolucion TEXT,
            fecha_devolucion_lider TEXT,
            revisado INTEGER DEFAULT 0,
            factura_original_id INTEGER, -- FK to facturas.id for replacements
            estado TEXT DEFAULT 'Activa' CHECK (estado IN ('Activa', 'Anulada', 'Reemplazada')),
            reemplazada_por_numero_factura TEXT, -- Stores the new invoice number if this one was replaced
            estado_auditoria TEXT DEFAULT 'Pendiente' CHECK (estado_auditoria IN ('Pendiente', 'Radicada OK', 'Devuelta por Auditor', 'Corregida por Legalizador')),
            observacion_auditor TEXT,
            tipo_error TEXT,
            fecha_reemplazo TEXT, -- Fecha en la que la factura original fue reemplazada (YYYY-MM-DD)
            fecha_entrega_radicador TEXT, -- Nueva columna para la fecha de entrega al radicador
            FOREIGN KEY (factura_original_id) REFERENCES facturas(id)
        );
    ''')

    # Tabla para detalles SOAT
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS soat_detalles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_id INTEGER UNIQUE NOT NULL,
            fecha_generacion TEXT NOT NULL, -- Fecha de generación de la factura SOAT
            FOREIGN KEY (factura_id) REFERENCES facturas(id)
        );
    ''')

    conn.commit()
    conn.close()

def obtener_credenciales_usuario(username):
    """
    Obtiene la contraseña y el rol de un usuario dado su nombre de usuario.

    Args:
        username (str): El nombre de usuario a buscar.

    Returns:
        tuple: (password, role) si el usuario es encontrado, None en caso contrario.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT password, role FROM usuarios WHERE username = ?", (username,))
    user_data = cursor.fetchone()
    conn.close()
    if user_data:
        return user_data['password'], user_data['role'] # Accede por nombre de columna
    return None

def guardar_factura(facturador, eps, numero_factura, fecha_generacion, area_servicio, fecha_hora_entrega):
    """
    Guarda una nueva factura en la base de datos.
    Retorna el ID de la nueva factura si es exitoso, None si el número de factura ya existe.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega))
        conn.commit()
        return cursor.lastrowid # Retorna el ID de la fila recién insertada
    except sqlite3.IntegrityError:
        # Esto ocurre si numero_factura ya existe (UNIQUE constraint)
        return None
    finally:
        conn.close()

def insertar_factura_bulk(numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega):
    """
    Inserta una factura en la base de datos, utilizada para carga masiva.
    Retorna el ID de la nueva factura o None si ya existe.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (numero_factura, area_servicio, facturador, fecha_generacion, eps, fecha_hora_entrega))
        conn.commit()
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        # Si el número de factura ya existe, no se inserta y se retorna None
        return None
    finally:
        conn.close()

def guardar_detalles_soat(factura_id, fecha_generacion_soat):
    """
    Guarda los detalles específicos de SOAT para una factura.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO soat_detalles (factura_id, fecha_generacion)
            VALUES (?, ?)
        ''', (factura_id, fecha_generacion_soat))
        conn.commit()
    except sqlite3.IntegrityError:
        # Esto ocurriría si ya existe un detalle SOAT para esta factura_id (UNIQUE constraint)
        print(f"Advertencia: Ya existe un detalle SOAT para la factura ID {factura_id}.")
    finally:
        conn.close()

def insertar_detalles_soat_bulk(factura_id, fecha_generacion_soat):
    """
    Inserta detalles SOAT para una factura, utilizada en carga masiva.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO soat_detalles (factura_id, fecha_generacion)
            VALUES (?, ?)
        ''', (factura_id, fecha_generacion_soat))
        conn.commit()
    except sqlite3.IntegrityError:
        # Esto ocurriría si ya existe un detalle SOAT para esta factura_id (UNIQUE constraint)
        pass # Ignorar silenciosamente si ya existe en carga masiva
    finally:
        conn.close()

def obtener_factura_por_id(factura_id):
    """
    Obtiene los datos completos de una factura por su ID, incluyendo detalles de la factura original
    si es una factura de reemplazo, y la fecha de entrega al radicador.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT
            f.id,
            f.numero_factura,
            f.area_servicio,
            f.facturador,
            f.fecha_generacion,
            f.eps,
            f.fecha_hora_entrega,
            f.tiene_correccion,
            f.descripcion_devolucion,
            f.fecha_devolucion_lider,
            f.revisado,
            f.factura_original_id,
            f.estado,
            f.reemplazada_por_numero_factura,
            f.estado_auditoria,
            f.observacion_auditor,
            f.tipo_error,
            f.fecha_reemplazo,
            fo.numero_factura AS num_fact_original_linked,
            fo.fecha_generacion AS fecha_gen_original_linked,
            f.fecha_entrega_radicador
        FROM facturas f
        LEFT JOIN facturas fo ON f.factura_original_id = fo.id
        WHERE f.id = ?
    ''', (factura_id,))
    factura_data = cursor.fetchone()
    conn.close()
    return factura_data

def actualizar_factura(
        factura_id, numero_factura, area_servicio, facturador,
        fecha_generacion, eps, fecha_hora_entrega, tiene_correccion,
        descripcion_devolucion, fecha_devolucion_lider, revisado,
        factura_original_id, estado, reemplazada_por_numero_factura,
        estado_auditoria, observacion_auditor, tipo_error, fecha_reemplazo):
    """
    Actualiza una factura existente en la base de datos.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE facturas SET
                numero_factura = ?,
                area_servicio = ?,
                facturador = ?,
                fecha_generacion = ?,
                eps = ?,
                fecha_hora_entrega = ?,
                tiene_correccion = ?,
                descripcion_devolucion = ?,
                fecha_devolucion_lider = ?,
                revisado = ?,
                factura_original_id = ?,
                estado = ?,
                reemplazada_por_numero_factura = ?,
                estado_auditoria = ?,
                observacion_auditor = ?,
                tipo_error = ?,
                fecha_reemplazo = ?
            WHERE id = ?
        ''', (
            numero_factura, area_servicio, facturador, fecha_generacion, eps,
            fecha_hora_entrega, tiene_correccion, descripcion_devolucion,
            fecha_devolucion_lider, revisado, factura_original_id, estado,
            reemplazada_por_numero_factura, estado_auditoria, observacion_auditor,
            tipo_error, fecha_reemplazo, factura_id
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # Esto ocurre si el nuevo numero_factura ya existe (UNIQUE constraint)
        return False
    finally:
        conn.close()

def actualizar_estado_auditoria_factura(factura_id, nuevo_estado, observacion_auditor, tipo_error):
    """
    Actualiza el estado de auditoría, la observación y el tipo de error de una factura.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE facturas SET
                estado_auditoria = ?,
                observacion_auditor = ?,
                tipo_error = ?
            WHERE id = ?
        ''', (nuevo_estado, observacion_auditor, tipo_error, factura_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error al actualizar estado de auditoría: {e}")
        return False
    finally:
        conn.close()

def actualizar_fecha_entrega_radicador(factura_id, fecha_hora_entrega):
    """
    Actualiza la fecha de entrega al radicador de una factura.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE facturas SET
                fecha_entrega_radicador = ?
            WHERE id = ?
        ''', (fecha_hora_entrega, factura_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error al actualizar fecha de entrega al radicador: {e}")
        return False
    finally:
        conn.close()

def eliminar_factura(factura_id):
    """
    Elimina una factura y sus detalles SOAT asociados de la base de datos.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Eliminar primero los detalles SOAT si existen
        cursor.execute("DELETE FROM soat_detalles WHERE factura_id = ?", (factura_id,))
        # Luego eliminar la factura
        cursor.execute("DELETE FROM facturas WHERE id = ?", (factura_id,))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error al eliminar factura: {e}")
        return False
    finally:
        conn.close()

def guardar_factura_reemplazo(factura_original_id, new_numero_factura, new_fecha_generacion,
                             original_area_servicio, original_facturador, original_eps, fecha_reemplazo):
    """
    Crea una nueva factura de reemplazo y actualiza el estado de la factura original.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 1. Marcar la factura original como 'Reemplazada' y guardar el nuevo número
        cursor.execute('''
            UPDATE facturas SET
                estado = 'Reemplazada',
                reemplazada_por_numero_factura = ?,
                fecha_reemplazo = ?
            WHERE id = ?
        ''', (new_numero_factura, fecha_reemplazo, factura_original_id))

        # 2. Insertar la nueva factura con el enlace a la original
        cursor.execute('''
            INSERT INTO facturas (numero_factura, area_servicio, facturador, fecha_generacion, eps,
                                  fecha_hora_entrega, factura_original_id, estado_auditoria)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'Pendiente')
        ''', (new_numero_factura, original_area_servicio, original_facturador, new_fecha_generacion,
              original_eps, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), factura_original_id))

        new_factura_id = cursor.lastrowid

        # Si el área de servicio es SOAT, guardar los detalles SOAT para la nueva factura
        if original_area_servicio == "SOAT":
            cursor.execute('''
                INSERT INTO soat_detalles (factura_id, fecha_generacion)
                VALUES (?, ?)
            ''', (new_factura_id, new_fecha_generacion)) # Usar la fecha de generación de la nueva factura

        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # Si el nuevo_numero_factura ya existe
        conn.rollback() # Revertir ambas operaciones
        return False
    except Exception as e:
        conn.rollback()
        print(f"Error al refacturar: {e}")
        return False
    finally:
        conn.close()


def cargar_facturas(search_term=None, search_column=None):
    """
    Carga todas las facturas 'Activas', 'Devueltas por Auditor' o 'Corregidas por Legalizador'
    de la base de datos, con la opción de filtrar por término y columna.
    Realiza un JOIN para obtener el numero_factura y fecha_generacion de la factura original
    cuando la factura actual es un reemplazo.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Base de la consulta SQL con JOIN para obtener la información de la factura original
    sql_query = '''
        SELECT
            f.id,
            f.numero_factura,
            f.area_servicio,
            f.facturador,
            f.fecha_generacion,
            f.eps,
            f.fecha_hora_entrega,
            f.tiene_correccion,
            f.descripcion_devolucion,
            f.fecha_devolucion_lider,
            f.revisado,
            f.factura_original_id,
            f.estado,
            f.reemplazada_por_numero_factura,
            f.estado_auditoria,
            f.observacion_auditor,
            f.tipo_error,
            f.fecha_reemplazo,
            fo.numero_factura AS num_fact_original_linked,
            fo.fecha_generacion AS fecha_gen_original_linked,
            f.fecha_entrega_radicador
        FROM facturas f
        LEFT JOIN facturas fo ON f.factura_original_id = fo.id
        WHERE f.estado IN ('Activa', 'Reemplazada') -- Mostrar activas y reemplazadas (incluyendo las originales reemplazadas)
    '''
    params = []

    # Aplicar filtro si se proporciona un search_term y search_column
    if search_term and search_column:
        search_term_like = f'%{search_term}%'
        if search_column == "numero_factura":
            # Buscar en el número de factura actual O en el número de factura de reemplazo O en el número de la factura original
            sql_query += '''
                AND (f.numero_factura LIKE ? OR f.reemplazada_por_numero_factura LIKE ? OR fo.numero_factura LIKE ?)
            '''
            params.extend([search_term_like, search_term_like, search_term_like])
        elif search_column == "facturador":
            sql_query += ' AND f.facturador LIKE ?'
            params.append(search_term_like)
        elif search_column == "eps":
            sql_query += ' AND f.eps LIKE ?'
            params.append(search_term_like)
        elif search_column == "area_servicio":
            sql_query += ' AND f.area_servicio LIKE ?'
            params.append(search_term_like)
        elif search_column == "estado_auditoria":
            sql_query += ' AND f.estado_auditoria LIKE ?'
            params.append(search_term_like)

    # Ordenar los resultados para que las facturas 'Devuelta por Auditor' y 'Corregida por Legalizador'
    # aparezcan al principio, seguidas por el resto.
    sql_query += '''
        ORDER BY
            CASE
                WHEN f.estado_auditoria = 'Devuelta por Auditor' THEN 1
                WHEN f.estado_auditoria = 'Corregida por Legalizador' THEN 2
                ELSE 3
            END,
            f.fecha_generacion DESC, f.id DESC
    '''

    cursor.execute(sql_query, tuple(params))
    facturas = cursor.fetchall()
    conn.close()

    # Convertir las filas a un formato de lista de tuplas para compatibilidad con la GUI
    # y para mantener la coherencia del resultado con versiones anteriores.
    # Accede a los campos por nombre (gracias a row_factory = sqlite3.Row)
    result = []
    for f in facturas:
        result.append((
            f['id'], f['numero_factura'], f['area_servicio'], f['facturador'],
            f['fecha_generacion'], f['eps'], f['fecha_hora_entrega'],
            f['tiene_correccion'], f['descripcion_devolucion'], f['fecha_devolucion_lider'],
            f['revisado'], f['factura_original_id'], f['estado'],
            f['reemplazada_por_numero_factura'], f['estado_auditoria'], f['observacion_auditor'],
            f['tipo_error'], f['fecha_reemplazo'],
            f['num_fact_original_linked'], f['fecha_gen_original_linked'], f['fecha_entrega_radicador']
        ))
    return result

def obtener_conteo_facturas_por_legalizador_y_eps():
    """
    Obtiene el conteo de facturas pendientes agrupadas por legalizador y EPS.
    Se consideran facturas pendientes aquellas con estado 'Activa'
    y estado_auditoria en 'Pendiente', 'Devuelta por Auditor', o 'Corregida por Legalizador'.

    Returns:
        list of tuples: Cada tupla contiene (facturador, eps, total_facturas_pendientes).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT
            facturador,
            eps,
            COUNT(id) AS total_facturas_pendientes
        FROM facturas
        WHERE estado = 'Activa' AND estado_auditoria IN ('Pendiente', 'Devuelta por Auditor', 'Corregida por Legalizador')
        GROUP BY facturador, eps
        ORDER BY facturador ASC, eps ASC, total_facturas_pendientes DESC;
    ''')
    result = cursor.fetchall()
    conn.close()
    # Convertir a lista de tuplas (nombre, eps, cantidad) para el frontend
    return [(row['facturador'], row['eps'], row['total_facturas_pendientes']) for row in result]
