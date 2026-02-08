-- 1. CREACIÓN DE TABLA NATIVA (NATIVE TABLE)
-- Importa los datos de la tabla externa (en GCS) hacia el almacenamiento interno de BigQuery.
CREATE OR REPLACE TABLE `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_regular` AS
SELECT * FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata`;

-- ---

-- # QUESTION 1

-- Cuenta el numero total de registros
SELECT COUNT(1)
FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata`;

-- ---

-- # QUESTION 2

-- 2. COMPARACIÓN DE RENDIMIENTO: TABLA EXTERNA
-- Escanea archivos directamente desde Google Cloud Storage. Observa los "Bytes processed" en la pestaña de resultados.
SELECT COUNT(DISTINCT PULocationID) 
FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata`;

-- 3. COMPARACIÓN DE RENDIMIENTO: TABLA NATIVA
-- Escanea datos almacenados en BigQuery. Compara los bytes con la consulta anterior; debería ser más eficiente (0 MB si es por metadatos o significativamente menos si lee la columna).
SELECT COUNT(DISTINCT PULocationID) 
FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_regular`;

-- ---

-- # QUESTION 3

-- 4. DEMOSTRACIÓN DE ALMACENAMIENTO COLUMNAR (UNA COLUMNA)
-- BigQuery solo lee la columna PULocationID. Revisa el costo estimado arriba a la derecha.
SELECT PULocationID 
FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_regular`;


-- 5. DEMOSTRACIÓN DE ALMACENAMIENTO COLUMNAR (DOS COLUMNAS)
-- Al agregar DOLocationID, los bytes procesados aumentarán proporcionalmente porque BigQuery lee un segundo archivo de columna.
SELECT PULocationID, DOLocationID
FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_regular`;


-- ---

-- # QUESTION 4

-- 6. FILTRADO BÁSICO
-- Cuenta cuántos viajes tuvieron una tarifa de 0. Ayuda a entender la calidad de los datos.
SELECT COUNT(1)
FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_regular`
WHERE fare_amount = 0;

-- ---

-- # QUESTION 5

-- 7. CREACIÓN DE TABLA OPTIMIZADA (PARTITIONED & CLUSTERED)
-- Crea una tabla física que divide los datos por fecha de entrega y organiza internamente por VendorID.
CREATE OR REPLACE TABLE `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_regular`;

-- ---

-- # QUESTION 6

-- 8. CONSULTA SOBRE TABLA NO OPTIMIZADA
-- Esta consulta realizará un "Full Table Scan" de la columna de fecha para encontrar el rango.
SELECT DISTINCT(VendorID)
FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_regular`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';


-- 9. CONSULTA SOBRE TABLA OPTIMIZADA
-- Gracias al "Partition Pruning", BigQuery solo lee las particiones de los primeros 15 días de marzo. Los bytes procesados serán drásticamente menores.
SELECT DISTINCT(VendorID)
FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- ---

-- # QUESTION 9

-- 10. ESCANEO TOTAL
-- Selecciona todo el dataset. Ten cuidado: esto consume la cuota de procesamiento basada en el tamaño total de la tabla.
SELECT *
FROM `ny-taxi-de-zoomcamp-484802.taxi_dataset_6ii19c15.yellow_tripdata_regular`;
