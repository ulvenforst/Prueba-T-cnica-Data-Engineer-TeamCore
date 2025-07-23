# Reporte de Benchmarks - ETL Streaming Log Processing

**Archivo procesado**: data\raw\sample.log.gz
**Fecha**: 2025-07-23 07:27:13

## Resultados por Método

| Método | Records/seg | Tiempo (s) | Memoria (MB) | Status |
|--------|-------------|------------|--------------|--------|
| pandas_streaming | 182,346 | 0.55 | 6.46 | SUCCESS |
| multiprocessing | 49,301 | 2.03 | 1.43 | SUCCESS |
| polars | 171,251 | 0.58 | 13.2 | SUCCESS |
| dask | 38,979 | 2.57 | 14.01 | SUCCESS |

## Análisis de Rendimiento

**Método más rápido**: pandas_streaming (182,346 records/seg)

**Más eficiente en memoria**: multiprocessing (1.43 MB)

## Configuración del Sistema

- CPU cores: 16
- Polars disponible: True
- Dask disponible: True
- PyArrow disponible: True
