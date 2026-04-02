"""
Capa de Visualización - Dashboard Streamlit
Proyecto: TECHTARIJA BI
Autor: Squad
Descripción: Dashboard interactivo con KPIs, gráficos y storytelling
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

# ============================================
# CONFIGURACIÓN DE LA PÁGINA
# ============================================

st.set_page_config(
    page_title="TECHTARIJA BI - Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# CARGA DE DATOS LIMPIOS
# ============================================

@st.cache_data
def cargar_datos():
    """Carga los datos limpios de la capa Silver"""
    
    data_path = 'E:/aaaaaaaaaaaaaaaa/proyecto-final/data/silver/'
    
    productos = pd.read_csv(data_path + 'productos_clean.csv')
    clientes = pd.read_csv(data_path + 'clientes_clean.csv')
    tecnicos = pd.read_csv(data_path + 'tecnicos_clean.csv')
    ventas = pd.read_csv(data_path + 'ventas_clean.csv')
    servicios = pd.read_csv(data_path + 'servicios_integrados.csv')
    cepalstat = pd.read_csv(data_path + 'cepalstat_clean.csv')
    
    return productos, clientes, tecnicos, ventas, servicios, cepalstat

# ============================================
# CÁLCULO DE KPIs
# ============================================

def calcular_kpis(servicios, ventas, productos):
    """Calcula los indicadores clave de gestión"""
    
    # KPI 1: Margen neto por servicio
    servicios['margen_neto'] = servicios['valor_venta_bs'] - servicios['costo_total_bs']
    servicios['margen_porcentaje'] = (servicios['margen_neto'] / servicios['valor_venta_bs']) * 100
    margen_promedio = servicios['margen_porcentaje'].mean()
    
    # KPI 2: Eficiencia de técnico (servicios/día)
    eficiencia = servicios.groupby('nombre').agg({
        'id_servicio': 'count',
        'fecha': 'nunique'
    }).reset_index()
    eficiencia['servicios_por_dia'] = eficiencia['id_servicio'] / eficiencia['fecha']
    eficiencia_promedio = eficiencia['servicios_por_dia'].mean()
    
    # KPI 3: Rotación de inventario
    ventas_con_productos = ventas.merge(productos, on='id_producto')
    rotacion = ventas_con_productos.groupby('categoria').agg({
        'cantidad': 'sum',
        'stock_actual': 'first'
    }).reset_index()
    rotacion['rotacion'] = rotacion['cantidad'] / rotacion['stock_actual']
    rotacion_promedio = rotacion['rotacion'].mean()
    
    # KPI 4: Tasa de re-servicios
    tasa_reservicios = (servicios['es_reservicio'].sum() / len(servicios)) * 100
    
    return {
        'margen_promedio': margen_promedio,
        'eficiencia_promedio': eficiencia_promedio,
        'rotacion_promedio': rotacion_promedio,
        'tasa_reservicios': tasa_reservicios
    }

# ============================================
# GRÁFICOS
# ============================================

def grafico_rentabilidad_zona(servicios):
    """Gráfico de rentabilidad por zona"""
    
    rentabilidad_zona = servicios.groupby('zona').agg({
        'margen_neto': 'sum',
        'valor_venta_bs': 'sum'
    }).reset_index()
    rentabilidad_zona = rentabilidad_zona.sort_values('margen_neto', ascending=False)
    
    fig = px.bar(
        rentabilidad_zona,
        x='zona',
        y='margen_neto',
        title='💰 Rentabilidad por Zona',
        labels={'zona': 'Zona', 'margen_neto': 'Margen Neto (Bs)'},
        color='margen_neto',
        color_continuous_scale='Viridis'
    )
    fig.update_layout(height=400)
    return fig


def grafico_eficiencia_tecnicos(servicios):
    """Gráfico de eficiencia por técnico"""
    
    eficiencia = servicios.groupby('nombre').agg({
        'id_servicio': 'count',
        'horas': 'sum'
    }).reset_index()
    eficiencia.columns = ['Técnico', 'Total Servicios', 'Horas Totales']
    eficiencia['Horas por Servicio'] = eficiencia['Horas Totales'] / eficiencia['Total Servicios']
    eficiencia = eficiencia.sort_values('Total Servicios', ascending=False)
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('📈 Total de Servicios', '⏱️ Horas por Servicio'),
        specs=[[{'type': 'bar'}, {'type': 'bar'}]]
    )
    
    fig.add_trace(
        go.Bar(x=eficiencia['Técnico'], y=eficiencia['Total Servicios'], name='Servicios', marker_color='#2E86AB'),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=eficiencia['Técnico'], y=eficiencia['Horas por Servicio'], name='Horas/Servicio', marker_color='#A23B72'),
        row=1, col=2
    )
    
    fig.update_layout(height=450, title_text="👨‍🔧 Eficiencia de Técnicos")
    return fig


def grafico_tasa_reservicios(servicios):
    """Gráfico de tasa de re-servicios"""
    
    reservicios_por_zona = servicios.groupby('zona').agg({
        'es_reservicio': 'sum',
        'id_servicio': 'count'
    }).reset_index()
    reservicios_por_zona['tasa'] = (reservicios_por_zona['es_reservicio'] / reservicios_por_zona['id_servicio']) * 100
    reservicios_por_zona = reservicios_por_zona.sort_values('tasa', ascending=False)
    
    fig = px.bar(
        reservicios_por_zona,
        x='zona',
        y='tasa',
        title='⚠️ Tasa de Re-servicios por Zona',
        labels={'zona': 'Zona', 'tasa': 'Tasa de Re-servicios (%)'},
        color='tasa',
        color_continuous_scale='Reds'
    )
    fig.update_layout(height=400)
    return fig


def grafico_rotacion_productos(ventas, productos):
    """Gráfico de rotación de inventario"""
    
    ventas_con_productos = ventas.merge(productos, on='id_producto')
    rotacion = ventas_con_productos.groupby('categoria').agg({
        'cantidad': 'sum',
        'stock_actual': 'first'
    }).reset_index()
    rotacion['rotacion'] = (rotacion['cantidad'] / rotacion['stock_actual']) * 100
    
    fig = px.pie(
        rotacion,
        values='rotacion',
        names='categoria',
        title='📦 Rotación de Inventario por Categoría',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig.update_layout(height=400)
    return fig


def grafico_comparacion_regional(cepalstat):
    """Gráfico comparativo con CEPALSTAT"""
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('🌎 Penetración de Banda Ancha (%)', '🏢 Empresas TIC por 100k hab.'),
        specs=[[{'type': 'bar'}, {'type': 'bar'}]]
    )
    
    cepalstat_sorted = cepalstat.sort_values('penetracion_banda_ancha_2024', ascending=False)
    
    fig.add_trace(
        go.Bar(x=cepalstat_sorted['pais'], y=cepalstat_sorted['penetracion_banda_ancha_2024'], name='Banda Ancha', marker_color='#1B998B'),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=cepalstat_sorted['pais'], y=cepalstat_sorted['empresas_TIC_por_100k'], name='Empresas TIC', marker_color='#FF9F1C'),
        row=1, col=2
    )
    
    fig.update_layout(height=450, title_text="📊 Comparativa Regional - Bolivia vs Cono Sur")
    return fig


def grafico_ventas_tendencia(ventas):
    """Gráfico de tendencia de ventas"""
    
    ventas['fecha'] = pd.to_datetime(ventas['fecha'])
    ventas_por_mes = ventas.groupby(ventas['fecha'].dt.to_period('M')).agg({
        'valor_bs': 'sum'
    }).reset_index()
    ventas_por_mes['fecha'] = ventas_por_mes['fecha'].astype(str)
    
    fig = px.line(
        ventas_por_mes,
        x='fecha',
        y='valor_bs',
        title='📈 Tendencia de Ventas',
        labels={'fecha': 'Mes', 'valor_bs': 'Ventas (Bs)'},
        markers=True
    )
    fig.update_traces(line_color='#2E86AB', line_width=3, marker_size=8)
    fig.update_layout(height=400)
    return fig

# ============================================
# MAIN - DASHBOARD
# ============================================

def main():
    # Título principal
    st.title("📊 TECHTARIJA BI - Dashboard de Inteligencia de Negocios")
    st.markdown("---")
    
    # Sidebar con filtros
    st.sidebar.image("https://img.icons8.com/color/96/000000/data-configuration.png", width=80)
    st.sidebar.title("🎛️ Filtros")
    st.sidebar.markdown("---")
    
    # Cargar datos
    with st.spinner("Cargando datos..."):
        productos, clientes, tecnicos, ventas, servicios, cepalstat = cargar_datos()
    
    # Filtros
    zonas_disponibles = ['Todas'] + sorted(servicios['zona'].unique().tolist())
    zona_seleccionada = st.sidebar.selectbox("📍 Filtrar por Zona", zonas_disponibles)
    
    if zona_seleccionada != 'Todas':
        servicios_filtrados = servicios[servicios['zona'] == zona_seleccionada]
        ventas_filtradas = ventas
    else:
        servicios_filtrados = servicios
        ventas_filtradas = ventas
    
    # Calcular KPIs
    kpis = calcular_kpis(servicios_filtrados, ventas_filtradas, productos)
    
    # Mostrar KPIs en tarjetas
    st.subheader("🎯 Indicadores Clave de Gestión (KPIs)")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📈 Margen Neto Promedio",
            value=f"{kpis['margen_promedio']:.1f}%",
            delta="Meta: >40%"
        )
    
    with col2:
        st.metric(
            label="👨‍🔧 Eficiencia Técnico",
            value=f"{kpis['eficiencia_promedio']:.2f} servicios/día",
            delta="Meta: >2.0"
        )
    
    with col3:
        st.metric(
            label="🔄 Rotación Inventario",
            value=f"{kpis['rotacion_promedio']:.2f}%",
            delta="Meta: >50%"
        )
    
    with col4:
        st.metric(
            label="⚠️ Tasa Re-servicios",
            value=f"{kpis['tasa_reservicios']:.1f}%",
            delta="Meta: <6%",
            delta_color="inverse"
        )
    
    st.markdown("---")
    
    # Fila 1: Rentabilidad y Eficiencia
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(grafico_rentabilidad_zona(servicios_filtrados), use_container_width=True)
    
    with col2:
        st.plotly_chart(grafico_eficiencia_tecnicos(servicios_filtrados), use_container_width=True)
    
    # Fila 2: Re-servicios y Rotación
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(grafico_tasa_reservicios(servicios_filtrados), use_container_width=True)
    
    with col2:
        st.plotly_chart(grafico_rotacion_productos(ventas_filtradas, productos), use_container_width=True)
    
    # Fila 3: Ventas y Comparativa Regional
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(grafico_ventas_tendencia(ventas_filtradas), use_container_width=True)
    
    with col2:
        st.plotly_chart(grafico_comparacion_regional(cepalstat), use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray;'>
        <p>📊 Dashboard desarrollado para TECHTARIJA BI | Alineado con ODS 9: Industria, Innovación e Infraestructura</p>
        <p>📅 Datos actualizados a Marzo 2026 | Fuente: Datos internos TECHTARIJA y CEPALSTAT</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()