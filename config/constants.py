# Opciones para los selectores y estados de la aplicación

# Opciones de Facturadores (Legalizadores)
FACTURADORES = [
    "ANDREA CEBALLOS",
    "ALEJANDRA BRAVO",
    "ALEJANDRA BURBANO",
    "ALEXIS ERAZO",
    "ANLLY HERNANDEZ",
    "BREYNER TEZ",
    "CAMILA IMBACHI",
    "CATHERIN NOVA",
    "CRISTIAN SAAVEDRA",
    "DALIANA SIERRA",
    "DANIEL DORADO",
    "DANY MORENO",
    "DIANA TELLEZ",
    "EMILCEN RODRIGUEZ",
    "FERNEY PULICHE",
    "GEAN VITERY",
    "GIOVANY PAZ",
    "JHOANA CARDENAS",
    "JHONY AYALA",
    "JUAN CUANTINDIOY",
    "JULIANA ARCINIEGAS",
    "KATHERINN PEREA",
    "LUCERO ESTRELLA",
    "LUCY MONTEZUMA",
    "LUISA OTALVARO",
    "LUZ TOBON",
    "MARGY POZO",
    "MARIA CASANOVA",
    "MARI CHAMORRO",
    "MARISOL BURGOS",
    "MAURICIO BURGOS",
    "MONICA CARVAJAL",
    "MONICA NASTACUAS",
    "NATALI LUCERO",
    "NICOLAS LEDESMA",
    "OSCAR MAYA",
    "ROSA ROMERO",
    "SOL BURBANO",
    "SULEIMA ACOSTA",
    "VIVIANA ROMO",
    "YESICA REVELO",
    "YINETH CLAROS",
    "YULLY GRIJALBA"
]

# Opciones de EPS
EPS_OPCIONES = [
    "ADRES",
    "ASMET SALUD EPS SAS",
    "ASOCIACION MUTUAL SER",
    "AXA COLPATRIA SEGUROS DE VIDA S A ARL",
    "AXA COLPATRIA SEGUROS SA",
    "CAJACOPI EPS S.A.S",
    "COLMEDICA MEDICINA PREPAGADA",
    "EMSSANAR E.P.S S.A.S.",
    "ENTIDAD PROMOTORA DE SALUD FAMISANAR SA S",
    "ENTIDAD PROMOTORA DE SALUD SERVICIO OCCIDENTAL DE SALUD S.A. S.O.S.",
    "ESM BATALLON DE ASPC NO 12 GR FERNANDO SERRANO",
    "EPS FAMILIAR DE COLOMBIA S.A.S.",
    "EPS SANITAS S.A",
    "FIDEICOMISOS PATRIMONIOS AUTONOMOS FIDUCIARIA LA PREVISORA S.A.",
    "LA EQUIDAD SEGUROS SOAT",
    "LA PREVISORA SA COMPANIA DE SEGUROS",
    "MALLAMAS EPS",
    "MUNDIAL DE SEGUROS",
    "NUEVA EPS",
    "REGIONAL DE ASEGURAMIENTO EN SALUD NO 2",
    "SALUD TOTAL SA EPS ARS",
    "SAVIA SALUD EPS",
    "SEGUROS COMERCIALES BOLIVAR",
    "SEGUROS DE VIDA DEL ESTADO",
    "SEGUROS DE VIDA SURAMERICANA S.A",
    "SEGUROS DEL ESTADO",
    "SRIA DE SALUD DPTAL DEL PTYO",
    "SURA"
]

# Opciones de Área de Servicio
AREA_SERVICIO_OPCIONES = [
    "SOAT",
    "Consulta Externa",
    "Urgencias",
    "Hospitalizacion",
    "Vacunacion"
]

# Opciones de Estado de Auditoría (actualizadas)
# "Radicada OK" ahora significa "Lista para Radicar"
# Se añade "En Radicador" y "Radicada y Aceptada"
ESTADO_AUDITORIA_OPCIONES = [
    "Pendiente",
    "Lista para Radicar",  # Antes "Radicada OK" (significado de lista para radicar)
    "En Radicador",        # Nuevo estado: la factura ya fue entregada al radicador
    "Devuelta por Auditor",
    "Corregida por Legalizador"
]

# Opciones de Tipo de Error (usadas cuando la factura es "Devuelta por Auditor")
TIPO_ERROR_OPCIONES = [
    "", # Opción vacía por defecto
    "ERROR DE FACTURACION",
    "TARIFA",
    "FIRMAS",
    "SOPORTES",
    "ERROR CONTRATO",
    "ERROR CRC",
    "SOPORTES NO COINCIDEN",
    "SOPORTES DE AUTORIZACION",
    "CODIGO DE AUTORIZACION",
    "SIN CARPETA",
    "REFACTURAR",
    "CORREGIR NOMBRES DE USUARIO",
    "AUTORIZACION EN LA FACTURA"
]
