## Taller 3

Usando docker compose:

1. Cree una instancia de una base de datos de preferencia.

2. Cree una instancia de Airflow.

3. Cree los DAG necesarios que le permitan:

    - Cargar datos de penguins, sin preprocesamiento!
	- Borrar contenido base de datos
	- Realizar entrenamiento de modelo usando datos de la base de datos (realizando procesamiento)

Bono
- Cree API que permita realizar inferencia al modelo entrenado

## Uso del repositorio

Clone este repositorio y ubíquese en el mismo con los siguientes comandos:

```shell
git clone https://github.com/Nirogu/JAV_MLOps_Taller3
cd JAV_MLOPS_TALLER3
```

Cree las carpetas de Airflow necesarias y asócielas con el usuario correcto:

```shell
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Levante Airflow (y una instancia MySQL) usando la configuración por defecto:

```shell
docker compose build
docker compose up airflow-init
docker compose up
```

Con Airflow ya ejecutándose, es posible acceder a la interfaz web en `http://localhost:8080`, por medio del usuario `airflow` y la contraseña `airflow`. Los 3 DAGs encontrados en la carpeta `dags` pueden administrarse desde esta interfaz.
