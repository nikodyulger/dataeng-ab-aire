## Poner en marcha

### Desde un contenedor

Crear la imagen

```bash
cd plata
docker build -t  transformer-plata .
```

Ejecutar el contenedor con variables de entorno guardando los ficheros descargados en tu máquina a través de un volumen
```bash
docker run --rm \
  --env-file .env \
  transformer-plata
```


```bash
cd oro
docker build -t  transformer-oro .
```

```bash
docker run --rm \
  --env-file .env \
  transformer-oro
```