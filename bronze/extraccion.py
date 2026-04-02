"""
Capa Bronze - Extracción de datos
Proyecto: TECHTARIJA BI
Autor: Squad
Descripción: Extrae datos de fuentes internas (CSV/Excel simulando SQL Server)
y externas (API CEPALSTAT)
"""

import pandas as pd
import requests
import json
import os
from datetime import datetime

# ============================================
# 1. GENERACIÓN DE DATOS SINTÉTICOS (SIMULA SQL SERVER)
# ============================================

def generar_datos_internos():
    """Genera datos sintéticos para TECHTARIJA (ventas, servicios, productos, tecnicos, clientes)"""
    
    # ===== 1.1 TABLA DE PRODUCTOS =====
    productos = pd.DataFrame({
        'id_producto': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        'nombre': [
            'GPS Trackmax X5', 'GPS Trackmax Pro', 'Cámara IP Hikvision DS-2CD', 
            'Cámara Analógica Dahua', 'Router MikroTik hAP', 'Switch TP-Link 8 puertos',
            'Cable UTP Cat6', 'NVR Hikvision 4 canales', 'Cámara IP TP-Link Tapo',
            'GPS Invoxia', 'Cámara IP Dahua PTZ', 'Router TPLink Archer', 
            'Switch Cisco 24 puertos', 'Kit GPS Vehicular', 'Cámara IP Xiaomi'
        ],
        'categoria': [
            'GPS', 'GPS', 'Camara', 'Camara', 'Red', 'Red', 'Accesorio',
            'NVR', 'Camara', 'GPS', 'Camara', 'Red', 'Red', 'GPS', 'Camara'
        ],
        'precio_costo_bs': [350, 520, 380, 250, 280, 180, 120, 650, 220, 480, 750, 300, 1200, 400, 310],
        'precio_venta_bs': [499, 799, 599, 399, 449, 299, 199, 999, 349, 699, 1199, 499, 1899, 649, 499],
        'stock_actual': [25, 12, 18, 30, 15, 40, 100, 8, 22, 10, 5, 20, 3, 18, 14]
    })
    
    # ===== 1.2 TABLA DE CLIENTES =====
    clientes = pd.DataFrame({
        'id_cliente': range(1, 21),
        'nombre': [
            'Transporte San Jorge', 'Empresa de Seguridad Atlas', 'Comercial Los Olivos',
            'Hotel Central Tarija', 'Colegio San Luis', 'Farmacia Santa Ana',
            'Restaurante El Mirador', 'Taller Mecánico Rápido', 'Librería Cultural',
            'Consultorio Médico Salud Total', 'Almacén Don José', 'Panadería La Paz',
            'Gimnasio Fit Center', 'Feretería El Constructor', 'Lavadero Express',
            'Café Boulevard', 'Centro de Cómputo Byte', 'Oficina Contable SUR',
            'Residencial Carmen', 'Tienda de Barrio La Esquina'
        ],
        'tipo_cliente': [
            'Corporativo', 'Corporativo', 'Comercio', 'Comercio', 'Institucion',
            'Comercio', 'Comercio', 'Taller', 'Comercio', 'Profesional',
            'Comercio', 'Comercio', 'Gimnasio', 'Comercio', 'Lavadero',
            'Comercio', 'Centro de Computo', 'Oficina', 'Residencial', 'Residencial'
        ],
        'zona': [
            'San Jorge', 'Centro', 'Los Olivos', 'Centro', 'San Jorge',
            'Centro', 'Los Olivos', 'San Andrés', 'Centro', 'Los Olivos',
            'Pampa Galana', 'Centro', 'San Jorge', 'San Andrés', 'Centro',
            'Los Olivos', 'Centro', 'Centro', 'Pampa Galana', 'San Andrés'
        ],
        'antiguedad_meses': [48, 36, 24, 18, 60, 12, 8, 4, 20, 15, 6, 3, 10, 2, 5, 7, 9, 14, 1, 3]
    })
    
    # ===== 1.3 TABLA DE TECNICOS =====
    tecnicos = pd.DataFrame({
        'id_tecnico': [1, 2, 3, 4],
        'nombre': ['Carlos Mamani', 'Ana Rojas', 'Jorge Paredes', 'Lucia Fernandez'],
        'zona_asignada': ['Centro', 'Los Olivos', 'San Jorge', 'San Andres'],
        'antiguedad_meses': [36, 24, 18, 6],
        'tarifa_hora_bs': [45, 40, 42, 38]
    })
    
    # ===== 1.4 TABLA DE VENTAS (30 registros) =====
    ventas_data = {
        'id_venta': range(1, 31),
        'fecha': pd.date_range('2025-01-01', '2025-03-30', periods=30),
        'id_producto': [1, 2, 3, 1, 4, 5, 2, 6, 3, 7, 1, 8, 4, 2, 9, 3, 5, 1, 10, 6, 2, 3, 7, 4, 1, 5, 8, 2, 3, 6],
        'id_cliente': [1, 2, 3, 1, 4, 5, 2, 6, 7, 8, 9, 10, 1, 2, 11, 3, 12, 4, 13, 5, 14, 6, 15, 7, 16, 8, 17, 9, 18, 10],
        'cantidad': [2, 1, 3, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1, 1, 3, 1, 1, 2, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1],
        'valor_bs': [998, 799, 1797, 499, 798, 449, 799, 598, 599, 199, 998, 999, 399, 799, 1047, 599, 449, 998, 699, 598, 799, 599, 199, 798, 499, 449, 999, 1598, 599, 299]
    }
    ventas = pd.DataFrame(ventas_data)
    
    # ===== 1.5 TABLA DE SERVICIOS (25 registros) =====
    servicios_data = {
        'id_servicio': range(1, 26),
        'fecha': pd.date_range('2025-01-05', '2025-03-28', periods=25),
        'id_tecnico': [1, 2, 3, 4, 1, 2, 3, 1, 4, 2, 1, 3, 2, 4, 1, 2, 3, 1, 4, 2, 1, 3, 2, 1, 4],
        'id_cliente': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 11, 12, 4, 5, 13, 14, 6, 7, 15, 8, 16, 9],
        'tipo_servicio': [
            'Instalacion GPS', 'Mantenimiento Camaras', 'Instalacion Red', 'Calibracion GPS',
            'Instalacion Camaras', 'Mantenimiento Red', 'Instalacion GPS', 'Reparacion Camara',
            'Instalacion Red', 'Mantenimiento GPS', 'Instalacion Camaras', 'Calibracion GPS',
            'Instalacion Red', 'Mantenimiento Camaras', 'Instalacion GPS', 'Reparacion Red',
            'Instalacion Camaras', 'Mantenimiento GPS', 'Instalacion Red', 'Calibracion GPS',
            'Instalacion Camaras', 'Instalacion GPS', 'Mantenimiento Red', 'Instalacion Camaras', 'Mantenimiento GPS'
        ],
        'horas': [4, 3, 5, 2, 4, 3, 4, 5, 3, 2, 4, 2, 3, 4, 5, 3, 4, 2, 3, 2, 4, 3, 2, 4, 2],
        'costo_combustible_bs': [50, 30, 80, 20, 45, 35, 55, 90, 40, 25, 60, 25, 40, 70, 100, 45, 65, 30, 50, 25, 55, 45, 35, 60, 30],
        'zona': [
            'San Jorge', 'Centro', 'Los Olivos', 'San Andres', 'Centro', 'Los Olivos',
            'San Jorge', 'Pampa Galana', 'Centro', 'San Andres', 'San Jorge', 'Centro',
            'Los Olivos', 'San Andres', 'Pampa Galana', 'Centro', 'San Jorge', 'Los Olivos',
            'San Andres', 'Centro', 'Pampa Galana', 'San Jorge', 'Centro', 'Los Olivos', 'San Andres'
        ],
        'es_reservicio': [0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]
    }
    servicios = pd.DataFrame(servicios_data)
    
    return productos, clientes, tecnicos, ventas, servicios


# ============================================
# 2. EXTRACCIÓN DE DATOS CEPALSTAT (API)
# ============================================

def extraer_cepalstat():
    """Extrae indicadores de CEPALSTAT - Penetración de banda ancha"""
    
    print("Extrayendo datos de CEPALSTAT...")
    
    # Datos simulados porque la API de CEPALSTAT requiere autenticación
    # En un proyecto real, se usaría: https://statistics.cepal.org/portal/cepalstat/index.html
    
    cepalstat_data = {
        'pais': ['Bolivia', 'Argentina', 'Chile', 'Uruguay', 'Paraguay', 'Peru', 'Brasil', 'Colombia'],
        'penetracion_banda_ancha_2024': [28.5, 78.3, 85.2, 79.1, 45.2, 52.3, 62.8, 58.4],
        'empresas_TIC_por_100k': [42.5, 98.3, 112.4, 95.6, 48.2, 55.3, 72.1, 65.8]
    }
    
    df_cepalstat = pd.DataFrame(cepalstat_data)
    
    # Guardar como CSV
    df_cepalstat.to_csv('E:/aaaaaaaaaaaaaaaa/proyecto-final/data/cepalstat_datos.csv', index=False)
    print("Datos CEPALSTAT guardados en data/cepalstat_datos.csv")
    
    return df_cepalstat


# ============================================
# 3. FUNCIÓN PRINCIPAL - EJECUTAR EXTRACCIÓN
# ============================================

def main():
    print("=" * 60)
    print("CAPA BRONZE - EXTRACCIÓN DE DATOS")
    print("Proyecto: TECHTARIJA BI")
    print("=" * 60)
    
    # 1. Generar datos internos
    print("\n[1/2] Generando datos internos de TECHTARIJA...")
    productos, clientes, tecnicos, ventas, servicios = generar_datos_internos()
    
    # Guardar datos internos como CSV
    data_path = 'E:/aaaaaaaaaaaaaaaa/proyecto-final/data/'
    os.makedirs(data_path, exist_ok=True)
    
    productos.to_csv(data_path + 'productos.csv', index=False)
    clientes.to_csv(data_path + 'clientes.csv', index=False)
    tecnicos.to_csv(data_path + 'tecnicos.csv', index=False)
    ventas.to_csv(data_path + 'ventas.csv', index=False)
    servicios.to_csv(data_path + 'servicios.csv', index=False)
    
    print(f"   - Productos: {len(productos)} registros")
    print(f"   - Clientes: {len(clientes)} registros")
    print(f"   - Técnicos: {len(tecnicos)} registros")
    print(f"   - Ventas: {len(ventas)} registros")
    print(f"   - Servicios: {len(servicios)} registros")
    print("   Guardado en carpeta 'data/'")
    
    # 2. Extraer datos CEPALSTAT
    print("\n[2/2] Extrayendo datos de CEPALSTAT...")
    df_cepalstat = extraer_cepalstat()
    print(f"   - Países: {len(df_cepalstat)} registros")
    
    print("\n" + "=" * 60)
    print("EXTRACCIÓN COMPLETADA EXITOSAMENTE")
    print("=" * 60)
    
    return productos, clientes, tecnicos, ventas, servicios, df_cepalstat


if __name__ == "__main__":
    main()