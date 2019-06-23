## Gestor dinámico de la granularidad

Este es el repositorio con todo el código fuente utilizado y implementado durante el proyecto ***Análisis e implementación de una gestión dinámica de la granularidad de las tareas en un sistema distribuido***, del alumno **Adrián Espejo Saldaña**.

### Estructura del repositorio

* dummy: Todo lo relacionado con la aplicación **Dummy**.
    * Dummy.py: código de la aplicación
    * gen_random_data.py: modelos de datos y funciones para generar datos aleatoriamente.
    
* earth: Todo lo relacionado con la aplicación **Interpolación de métricas en diferentes longitudes y latitudes**.
    * classes.py: modelo de datos y funciones auxiliares.
    * earth.py: código de la aplicación.
    * fulldb.csv: datos de entrada en un fichero CSV (4 MB).
    * fulldb.cql: instrucciones en CQL (Cassandra Query Language) para insertar los datos del fichero CSV.
    
* ip_relationships: Todo lo relacionado con la aplicación **Relaciones de IPs con fechas solapadas**.
    * IP_relationships_overlapping_dates.py: código de la aplicación.
    * data.csv: datos de entrada en un fichero CSV (5,4 MB).
    * data_model.py: modelo de datos.
    
* partitioners: implementación de los **Gestores de la granularidad** realizada para el proyecto, incluyendo los tests.
    * partitioner1.py: primera versión del gestor dinámico.
    * partitioner2.py: segunda versión del gestor dinámico.
    * partitioner3.py: tercera versión del gestor dinámico.
    * partitioner4.py: cuarta versión del gestor dinámico.
    * api.py: implementación de la api de PyCOMPSs para Hecuba.
    * partitioner_tests.py: tests unitarios del particionador.
