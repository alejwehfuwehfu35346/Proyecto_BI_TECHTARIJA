"""
Capa Silver - Limpieza y Transformación de Datos
Proyecto: TECHTARIJA BI
Descripción: Limpieza de datos, tratamiento de nulos, estandarización
"""

import pandas as pd
import numpy as np
import os

# ============================================
# 1. CARGAR DATOS DE LA CAPA BRONZE
# ============================================

def cargar_datos():
    """Carga los datos desde la carpeta data/"""
    
    data_path = 'E:/aaaaaaaaaaaaaaaa/proyecto-final/data/'
    
    productos = pd.read_csv(data_path + 'productos.csv')
    clientes = pd.read_csv(data_path + 'clientes.csv')
    tecnicos = pd.read_csv(data_path + 'tecnicos.csv')
    ventas = pd.read_csv(data_path + 'ventas.csv')
    servicios = pd.read_csv(data_path + 'servicios.csv')
    cepalstat = pd.read_csv(data_path + 'cepalstat_datos.csv')
    
    print("Datos cargados exitosamente:")
    print(f"  - Productos: {len(productos)} registros")
    print(f"  - Clientes: {len(clientes)} registros")
    print(f"  - Técnicos: {len(tecnicos)} registros")
    print(f"  - Ventas: {len(ventas)} registros")
    print(f"  - Servicios: {len(servicios)} registros")
    print(f"  - CEPALSTAT: {len(cepalstat)} registros")
    
    return productos, clientes, tecnicos, ventas, servicios, cepalstat


# ============================================
# 2. LIMPIEZA DE TABLAS
# ============================================

def limpiar_productos(df):
    """Limpieza de tabla productos"""
    print("\n--- Limpiando tabla PRODUCTOS ---")
    
    print(f"  Nulos antes: {df.isnull().sum().sum()}")
    df = df.drop_duplicates()
    df['categoria'] = df['categoria'].str.upper()
    df['margen_bs'] = df['precio_venta_bs'] - df['precio_costo_bs']
    df['margen_porcentaje'] = (df['margen_bs'] / df['precio_venta_bs']) * 100
    
    print(f"  Nulos después: {df.isnull().sum().sum()}")
    print(f"  Registros finales: {len(df)}")
    return df


def limpiar_clientes(df):
    """Limpieza de tabla clientes"""
    print("\n--- Limpiando tabla CLIENTES ---")
    
    print(f"  Nulos antes: {df.isnull().sum().sum()}")
    df = df.drop_duplicates()
    df['zona'] = df['zona'].str.upper().str.strip()
    df['tipo_cliente'] = df['tipo_cliente'].str.upper().str.strip()
    
    zona_mapping = {
        'CENTRO': 'Centro',
        'LOS OLIVOS': 'Los Olivos', 
        'SAN JORGE': 'San Jorge',
        'SAN ANDRES': 'San Andrés',
        'PAMPA GALANA': 'Pampa Galana'
    }
    df['zona'] = df['zona'].map(zona_mapping).fillna(df['zona'])
    
    print(f"  Nulos después: {df.isnull().sum().sum()}")
    print(f"  Registros finales: {len(df)}")
    return df


def limpiar_tecnicos(df):
    """Limpieza de tabla técnicos"""
    print("\n--- Limpiando tabla TECNICOS ---")
    
    print(f"  Nulos antes: {df.isnull().sum().sum()}")
    df = df.drop_duplicates()
    df['nombre'] = df['nombre'].str.upper().str.strip()
    df['zona_asignada'] = df['zona_asignada'].str.upper().str.strip()
    
    print(f"  Nulos después: {df.isnull().sum().sum()}")
    print(f"  Registros finales: {len(df)}")
    return df


def limpiar_ventas(df):
    """Limpieza de tabla ventas"""
    print("\n--- Limpiando tabla VENTAS ---")
    
    print(f"  Nulos antes: {df.isnull().sum().sum()}")
    df = df.drop_duplicates()
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['mes'] = df['fecha'].dt.month
    df['anio'] = df['fecha'].dt.year
    
    print(f"  Nulos después: {df.isnull().sum().sum()}")
    print(f"  Registros finales: {len(df)}")
    return df


def limpiar_servicios(df):
    """Limpieza de tabla servicios"""
    print("\n--- Limpiando tabla SERVICIOS ---")
    
    print(f"  Nulos antes: {df.isnull().sum().sum()}")
    df = df.drop_duplicates()
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['tipo_servicio'] = df['tipo_servicio'].str.upper().str.strip()
    df['zona'] = df['zona'].str.upper().str.strip()
    
    zona_mapping = {
        'CENTRO': 'Centro',
        'LOS OLIVOS': 'Los Olivos',
        'SAN JORGE': 'San Jorge', 
        'SAN ANDRES': 'San Andrés',
        'PAMPA GALANA': 'Pampa Galana'
    }
    df['zona'] = df['zona'].map(zona_mapping).fillna(df['zona'])
    df['mes'] = df['fecha'].dt.month
    df['anio'] = df['fecha'].dt.year
    
    print(f"  Nulos después: {df.isnull().sum().sum()}")
    print(f"  Registros finales: {len(df)}")
    return df


def limpiar_cepalstat(df):
    """Limpieza de tabla CEPALSTAT"""
    print("\n--- Limpiando tabla CEPALSTAT ---")
    
    print(f"  Nulos antes: {df.isnull().sum().sum()}")
    df = df.drop_duplicates()
    df['pais'] = df['pais'].str.upper().str.strip()
    
    print(f"  Nulos después: {df.isnull().sum().sum()}")
    print(f"  Registros finales: {len(df)}")
    return df


# ============================================
# 3. INTEGRACIÓN DE DATOS
# ============================================

def integrar_datos(ventas, servicios, tecnicos):
    """Integra las tablas para análisis"""
    
    print("\n--- Integrando datos para análisis ---")
    
    # Unir servicios con técnicos para obtener tarifa
    servicios_con_tecnico = servicios.merge(
        tecnicos[['id_tecnico', 'tarifa_hora_bs', 'nombre']],
        on='id_tecnico',
        how='left'
    )
    
    # Calcular costo de mano de obra
    servicios_con_tecnico['costo_mano_obra_bs'] = servicios_con_tecnico['horas'] * servicios_con_tecnico['tarifa_hora_bs']
    
    # Calcular costo total del servicio
    servicios_con_tecnico['costo_total_bs'] = servicios_con_tecnico['costo_combustible_bs'] + servicios_con_tecnico['costo_mano_obra_bs']
    
    # ASIGNAR VALOR DE INGRESO POR SERVICIO (simulado basado en horas y tipo)
    # Precios base por tipo de servicio
    precios_servicio = {
        'INSTALACION GPS': 450,
        'MANTENIMIENTO CAMARAS': 350,
        'INSTALACION RED': 400,
        'CALIBRACION GPS': 250,
        'INSTALACION CAMARAS': 500,
        'MANTENIMIENTO RED': 300,
        'REPARACION CAMARA': 380,
        'MANTENIMIENTO GPS': 320,
        'REPARACION RED': 350
    }
    
    servicios_con_tecnico['valor_venta_bs'] = servicios_con_tecnico['tipo_servicio'].map(precios_servicio)
    
    # Si algún tipo no tiene precio, asignar 350 por defecto
    servicios_con_tecnico['valor_venta_bs'] = servicios_con_tecnico['valor_venta_bs'].fillna(350)
    
    print(f"  Datos integrados: {len(servicios_con_tecnico)} registros")
    
    return servicios_con_tecnico


# ============================================
# 4. FUNCIÓN PRINCIPAL
# ============================================

def main():
    print("=" * 60)
    print("CAPA SILVER - LIMPIEZA Y TRANSFORMACIÓN")
    print("Proyecto: TECHTARIJA BI")
    print("=" * 60)
    
    # 1. Cargar datos
    print("\n[1/4] Cargando datos...")
    productos, clientes, tecnicos, ventas, servicios, cepalstat = cargar_datos()
    
    # 2. Limpiar cada tabla
    print("\n[2/4] Limpiando tablas...")
    productos_clean = limpiar_productos(productos)
    clientes_clean = limpiar_clientes(clientes)
    tecnicos_clean = limpiar_tecnicos(tecnicos)
    ventas_clean = limpiar_ventas(ventas)
    servicios_clean = limpiar_servicios(servicios)
    cepalstat_clean = limpiar_cepalstat(cepalstat)
    
    # 3. Integrar datos
    print("\n[3/4] Integrando datos...")
    servicios_integrados = integrar_datos(ventas_clean, servicios_clean, tecnicos_clean)
    
    # 4. Guardar datos limpios
    print("\n[4/4] Guardando datos limpios...")
    silver_path = 'E:/aaaaaaaaaaaaaaaa/proyecto-final/data/silver/'
    os.makedirs(silver_path, exist_ok=True)
    
    productos_clean.to_csv(silver_path + 'productos_clean.csv', index=False)
    clientes_clean.to_csv(silver_path + 'clientes_clean.csv', index=False)
    tecnicos_clean.to_csv(silver_path + 'tecnicos_clean.csv', index=False)
    ventas_clean.to_csv(silver_path + 'ventas_clean.csv', index=False)
    servicios_clean.to_csv(silver_path + 'servicios_clean.csv', index=False)
    servicios_integrados.to_csv(silver_path + 'servicios_integrados.csv', index=False)
    cepalstat_clean.to_csv(silver_path + 'cepalstat_clean.csv', index=False)
    
    print(f"  Datos guardados en: {silver_path}")
    
    print("\n" + "=" * 60)
    print("LIMPIEZA COMPLETADA EXITOSAMENTE")
    print("=" * 60)


if __name__ == "__main__":
    main()