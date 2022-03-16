# Data Engineer - Challenge

## ETL Job
El código del job ETL está en la carpeta src/. Consiste de 1 script principal (`main.py`)y un paquete (`de_challenge`) que contiene las funciones de normalización y otras de apoyo al proceso.

## Data Model
El modelo de datos se encuentra en la carpeta DataModel/, en formato draw.io y PNG.
La herramienta fue elegida por su simplicidad de uso, precio y posibilidad de visualizar y diseñar archivos tanto en la web como en el escritorio y en VS Code.
El modelo contiene las siguientes entidades:

* company | corresponde al archivo final companies.csv
* console | corresponde al archivo consoles.csv
* title | corresponde al archivo final titles.csv
* scores | corresponde al archivo scores.csv

## Deployment 
El desarrollo fue dockerizado para su mejor portabilidad. Para ejecutar el ETL, se deben seguir los siguientes pasos:

```
docker volume create dechallenge

docker build -t dechallenge:latest -f Deployment/Dockerfile .

docker run
    --name dechallenge
    --mount source=dechallenge,target=/usr/app
    dechallenge:latest
```

Se provee un archivo `exec.sh` también de referencia de uso.