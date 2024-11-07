# Pipeline ETL de Ventas con Apache Airflow y Google BigQuery

Este proyecto implementa un pipeline ETL (Extracción, Transformación, Carga) para datos de ventas utilizando Apache Airflow. El pipeline extrae datos de ventas, los transforma calculando el ingreso total y los carga en Google BigQuery para análisis posteriores. El pipeline está diseñado para ejecutarse de forma diaria y maneja errores de manera eficiente.

## Descripción del Proyecto

El pipeline ETL realiza los siguientes pasos:
1. **Extracción**: Recupera datos de ventas de muestra, que incluyen nombres de productos, cantidades, precios y fechas de ventas, y los guarda como un archivo CSV.
2. **Transformación**: Lee los datos extraídos, calcula el ingreso total por producto y guarda los datos transformados en un nuevo archivo CSV.
3. **Carga**: Sube los datos transformados a Google Cloud Storage (GCS) y luego los carga en una tabla de BigQuery para su almacenamiento y análisis.

Este pipeline está gestionado por Apache Airflow, que programa y orquesta cada tarea en el proceso ETL.

## Flujo de Trabajo del Pipeline

El DAG (Grafo Acíclico Dirigido) para este pipeline consta de cuatro tareas principales:
1. **`extract_data`**: 
   - Extrae los datos de ventas y los guarda como `raw_sales_data.csv` en el directorio `/tmp`.
2. **`transform_data`**:
   - Lee los datos de ventas sin procesar, calcula el `total_revenue` (cantidad * precio) y guarda el resultado como `transformed_sales_data.csv`.
3. **`load_transformed_to_gcs`**:
   - Sube los datos transformados a un bucket específico de Google Cloud Storage.
4. **`load_to_bigquery`**:
   - Carga los datos transformados desde GCS a una tabla de BigQuery para análisis.

### Estructura del DAG

El pipeline está organizado de la siguiente manera:

```plaintext
extract_data -> transform_data -> load_transformed_to_gcs -> load_to_bigquery
