/*
Capa Gold - Modelado Estrella y KPIs
Proyecto: TECHTARIJA BI
Base de datos: SQL Server
*/

-- ============================================
-- 1. CREAR BASE DE DATOS
-- ============================================

CREATE DATABASE IF NOT EXISTS TechTarijaBI;
GO

USE TechTarijaBI;
GO

-- ============================================
-- 2. TABLAS DIMENSIÓN (Dimensiones)
-- ============================================

-- Dimensión Productos
CREATE TABLE DimProducto (
    id_producto INT PRIMARY KEY,
    nombre VARCHAR(100),
    categoria VARCHAR(50),
    precio_costo_bs DECIMAL(10,2),
    precio_venta_bs DECIMAL(10,2),
    stock_actual INT,
    margen_bs DECIMAL(10,2),
    margen_porcentaje DECIMAL(5,2)
);

-- Dimensión Clientes
CREATE TABLE DimCliente (
    id_cliente INT PRIMARY KEY,
    nombre VARCHAR(100),
    tipo_cliente VARCHAR(50),
    zona VARCHAR(50),
    antiguedad_meses INT
);

-- Dimensión Técnicos
CREATE TABLE DimTecnico (
    id_tecnico INT PRIMARY KEY,
    nombre VARCHAR(100),
    zona_asignada VARCHAR(50),
    antiguedad_meses INT,
    tarifa_hora_bs DECIMAL(10,2)
);

-- Dimensión Tiempo (para análisis temporal)
CREATE TABLE DimTiempo (
    id_tiempo INT PRIMARY KEY,
    fecha DATE,
    anio INT,
    mes INT,
    nombre_mes VARCHAR(20),
    trimestre INT,
    dia_semana INT
);

-- Dimensión Zona
CREATE TABLE DimZona (
    id_zona INT PRIMARY KEY,
    nombre_zona VARCHAR(50),
    tipo_zona VARCHAR(50),
    factor_logistico DECIMAL(3,2)  -- 1.0 = bajo, 1.5 = medio, 2.0 = alto
);

-- ============================================
-- 3. TABLA HECHO (FactTable) - Ventas y Servicios
-- ============================================

CREATE TABLE HechoOperaciones (
    id_operacion INT IDENTITY(1,1) PRIMARY KEY,
    fecha DATE,
    id_producto INT,
    id_cliente INT,
    id_tecnico INT,
    id_zona INT,
    id_tiempo INT,
    tipo_operacion VARCHAR(20),  -- 'VENTA' o 'SERVICIO'
    cantidad INT,
    valor_venta_bs DECIMAL(10,2),
    horas_trabajadas DECIMAL(5,2),
    costo_combustible_bs DECIMAL(10,2),
    costo_mano_obra_bs DECIMAL(10,2),
    costo_total_bs DECIMAL(10,2),
    es_reservicio BIT,
    FOREIGN KEY (id_producto) REFERENCES DimProducto(id_producto),
    FOREIGN KEY (id_cliente) REFERENCES DimCliente(id_cliente),
    FOREIGN KEY (id_tecnico) REFERENCES DimTecnico(id_tecnico),
    FOREIGN KEY (id_zona) REFERENCES DimZona(id_zona),
    FOREIGN KEY (id_tiempo) REFERENCES DimTiempo(id_tiempo)
);

-- ============================================
-- 4. INSERTAR DATOS EN DIMENSIONES
-- ============================================

-- Insertar zonas
INSERT INTO DimZona (id_zona, nombre_zona, tipo_zona, factor_logistico) VALUES
(1, 'Centro', 'Urbano', 1.0),
(2, 'Los Olivos', 'Residencial', 1.5),
(3, 'San Jorge', 'Residencial', 1.5),
(4, 'San Andrés', 'Periférico', 2.0),
(5, 'Pampa Galana', 'Periférico', 2.5);

-- Poblar DimTiempo (2024-2026)
DECLARE @StartDate DATE = '2024-01-01';
DECLARE @EndDate DATE = '2026-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO DimTiempo (id_tiempo, fecha, anio, mes, nombre_mes, trimestre, dia_semana)
    VALUES (
        CAST(CONVERT(VARCHAR(8), @StartDate, 112) AS INT),
        @StartDate,
        YEAR(@StartDate),
        MONTH(@StartDate),
        DATENAME(MONTH, @StartDate),
        DATEPART(QUARTER, @StartDate),
        DATEPART(WEEKDAY, @StartDate)
    );
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END;

-- ============================================
-- 5. KPIs - Indicadores Clave de Gestión
-- ============================================

-- KPI 1: Margen Neto por Servicio
CREATE VIEW VW_KPI_MargenServicio AS
SELECT 
    h.id_operacion,
    h.fecha,
    d.nombre_zona,
    t.nombre AS nombre_tecnico,
    h.valor_venta_bs AS ingreso_servicio,
    h.costo_total_bs,
    (h.valor_venta_bs - h.costo_total_bs) AS margen_neto_bs,
    CASE 
        WHEN h.valor_venta_bs > 0 
        THEN ((h.valor_venta_bs - h.costo_total_bs) / h.valor_venta_bs) * 100 
        ELSE 0 
    END AS margen_porcentaje
FROM HechoOperaciones h
JOIN DimZona d ON h.id_zona = d.id_zona
JOIN DimTecnico t ON h.id_tecnico = t.id_tecnico
WHERE h.tipo_operacion = 'SERVICIO';

-- KPI 2: Eficiencia de Técnico (servicios/día)
CREATE VIEW VW_KPI_EficienciaTecnico AS
SELECT 
    t.id_tecnico,
    t.nombre,
    COUNT(h.id_operacion) AS total_servicios,
    COUNT(DISTINCT h.fecha) AS dias_trabajados,
    CAST(COUNT(h.id_operacion) AS FLOAT) / NULLIF(COUNT(DISTINCT h.fecha), 0) AS servicios_por_dia
FROM HechoOperaciones h
JOIN DimTecnico t ON h.id_tecnico = t.id_tecnico
WHERE h.tipo_operacion = 'SERVICIO'
GROUP BY t.id_tecnico, t.nombre;

-- KPI 3: Rotación de Inventario
CREATE VIEW VW_KPI_RotacionInventario AS
SELECT 
    p.id_producto,
    p.nombre,
    p.categoria,
    SUM(h.cantidad) AS unidades_vendidas,
    p.stock_actual,
    CASE 
        WHEN p.stock_actual > 0 
        THEN SUM(h.cantidad) / p.stock_actual 
        ELSE 0 
    END AS rotacion_inventario
FROM HechoOperaciones h
JOIN DimProducto p ON h.id_producto = p.id_producto
WHERE h.tipo_operacion = 'VENTA'
GROUP BY p.id_producto, p.nombre, p.categoria, p.stock_actual;

-- KPI 4: Tasa de Re-servicios
CREATE VIEW VW_KPI_TasaReservicios AS
SELECT 
    d.nombre_zona,
    t.nombre AS nombre_tecnico,
    COUNT(h.id_operacion) AS total_servicios,
    SUM(CASE WHEN h.es_reservicio = 1 THEN 1 ELSE 0 END) AS total_reservicios,
    (SUM(CASE WHEN h.es_reservicio = 1 THEN 1 ELSE 0 END) / COUNT(h.id_operacion)) * 100 AS tasa_reservicios_porcentaje
FROM HechoOperaciones h
JOIN DimZona d ON h.id_zona = d.id_zona
JOIN DimTecnico t ON h.id_tecnico = t.id_tecnico
WHERE h.tipo_operacion = 'SERVICIO'
GROUP BY d.nombre_zona, t.nombre;

-- ============================================
-- 6. OKR (Objective Key Result) - Objetivo Estratégico
-- ============================================
-- Objetivo: Reducir la tasa de re-servicios en un 50% en 6 meses
-- Key Result 1: Reducir tasa de re-servicios del 12% al 6%
-- Key Result 2: Aumentar eficiencia de técnicos a 2 servicios/día
-- Key Result 3: Reducir días de inventario de 150 a 90

CREATE VIEW VW_OKR_Seguimiento AS
SELECT 
    'KR1 - Tasa Re-servicios' AS key_result,
    12.0 AS valor_inicial,
    6.0 AS valor_meta,
    (SELECT AVG(tasa_reservicios_porcentaje) FROM VW_KPI_TasaReservicios) AS valor_actual,
    CASE 
        WHEN (SELECT AVG(tasa_reservicios_porcentaje) FROM VW_KPI_TasaReservicios) <= 6.0 
        THEN 'ALCANZADO'
        ELSE 'EN PROGRESO'
    END AS estado
UNION ALL
SELECT 
    'KR2 - Eficiencia Técnico',
    1.5,
    2.0,
    (SELECT AVG(servicios_por_dia) FROM VW_KPI_EficienciaTecnico),
    CASE 
        WHEN (SELECT AVG(servicios_por_dia) FROM VW_KPI_EficienciaTecnico) >= 2.0 
        THEN 'ALCANZADO'
        ELSE 'EN PROGRESO'
    END
UNION ALL
SELECT 
    'KR3 - Rotación Inventario',
    0.67,  -- 150 días = 0.67 rotaciones/mes
    1.33,  -- 90 días = 1.33 rotaciones/mes
    (SELECT AVG(rotacion_inventario) FROM VW_KPI_RotacionInventario),
    CASE 
        WHEN (SELECT AVG(rotacion_inventario) FROM VW_KPI_RotacionInventario) >= 1.33 
        THEN 'ALCANZADO'
        ELSE 'EN PROGRESO'
    END;

-- ============================================
-- 7. REPORTE DE RESULTADOS
-- ============================================

-- Reporte de Rentabilidad por Zona
CREATE VIEW VW_Reporte_RentabilidadZona AS
SELECT 
    d.nombre_zona,
    COUNT(h.id_operacion) AS total_servicios,
    SUM(h.valor_venta_bs) AS ingreso_total,
    SUM(h.costo_total_bs) AS costo_total,
    SUM(h.valor_venta_bs - h.costo_total_bs) AS beneficio_total,
    AVG(h.valor_venta_bs - h.costo_total_bs) AS beneficio_promedio
FROM HechoOperaciones h
JOIN DimZona d ON h.id_zona = d.id_zona
WHERE h.tipo_operacion = 'SERVICIO'
GROUP BY d.nombre_zona
ORDER BY beneficio_total DESC;

-- Reporte de Comparación Regional (Bolivia vs Cono Sur)
CREATE VIEW VW_Reporte_ComparacionRegional AS
SELECT * FROM cepalstat_clean;