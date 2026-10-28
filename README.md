# Procesamiento de Datos con Apache Spark - SECOP II Colombia

**Estudiante:** Diego Alejandro González Lara  
**Curso:** Big Data (202016911) - Tarea 3  
**Universidad:** Universidad Nacional Abierta y a Distancia (UNAD) - Ingeniería de Sistemas (2026)

## Descripción de la Solución
Implementación en **Apache Spark (PySpark)** para el procesamiento por lotes (*batch*), limpieza, transformación con RDDs y Análisis Exploratorio de Datos (EDA) con DataFrames sobre el conjunto de datos público **SECOP II - Contratos Electrónicos (`jbjy-vk9h`)** del portal **Datos Abiertos de Colombia (`www.datos.gov.co`)**.

## Resultados Obtenidos (Muestra de 50,000 registros)
* **Registros limpios y válidos:** 48,672 contratos estatales.
* **Top Modalidades (RDDs `map` y `reduceByKey`):** Contratación directa (37,551), Régimen especial (6,323) y Mínima cuantía (2,605).
* **Top Departamentos por Ejecución Presupuestal (DataFrames):** Bogotá D.C. (16,284 contratos), Valle del Cauca (5,114), Antioquia (4,647), Santander (2,322) y Cundinamarca (2,197).
* **Top Sectores:** Servicio Público (12,526), Salud y Protección Social (6,797) y Educación Nacional (2,635).

## Instrucciones de Ejecución
1. Descargar el dataset desde la API oficial de Datos Abiertos de Colombia:
   ```bash
   wget -O secop_ii_contratos.csv "[https://www.datos.gov.co/resource/jbjy-vk9h.csv](https://www.datos.gov.co/resource/jbjy-vk9h.csv)?\$limit=50000"
