#!/bin/sh
# Borra el modelo de lectura y lo reconstruye leyendo Kafka desde el inicio.
set -e
docker compose stop proyector
docker compose run --rm --no-deps init node scripts/rebuild.js
docker compose start proyector
