#!/usr/bin/env bash
# Tarea 2 - Redis + HBase. Corre todo de punta a punta sobre una maquina limpia:
#
#   ./run.sh              todo: contenedores, venv, carga historica, experimentos D, hora en vivo, verificacion, preguntas
#   ./run.sh pipeline     solo B: carga historica + hora en vivo + verificacion
#   ./run.sh fallas       solo D.1 y D.2 (necesita la carga historica hecha)
#   ./run.sh preguntas    solo P1..P6 (necesita pipeline hecho)
#   ./run.sh verificar    solo B.3
#
# Necesita Docker con compose v2 y Python >= 3.10. No hay rutas que editar.
set -euo pipefail
cd "$(dirname "$0")"
PARTE="${1:-todo}"
export PYTHONIOENCODING=utf-8
INICIO=$(date +%s)

echo "== Contenedores (la primera vez construye la imagen de HBase, ~340 MB) =="
docker compose up -d --wait

# --- Python en ambiente virtual -------------------------------------------------------------
if [ ! -d .venv ]; then
  for cand in python3 python; do   # en Windows "python3" puede ser el atajo de la Store que no funciona
    if command -v "$cand" >/dev/null 2>&1 && "$cand" -c 'import sys; assert sys.version_info >= (3, 10)' 2>/dev/null; then
      "$cand" -m venv .venv && break
    fi
  done
fi
if [ -x .venv/bin/python ]; then PY=.venv/bin/python; else PY=.venv/Scripts/python.exe; fi
"$PY" -m pip install -q --disable-pip-version-check -r requirements.txt

paso() { local titulo="$1"; shift; echo; echo "== $titulo =="; local t=$(date +%s); "$PY" -m "$@"; echo "   ($(( $(date +%s) - t )) s)"; }
verificar() { paso "B.3 Verificacion" pipeline.verificar || echo "!! la verificacion encontro diferencias: ver resultados/b3_verificacion.md"; }

case "$PARTE" in
  todo)
    paso "B.1 Carga historica (864.000 eventos -> HBase)" pipeline.carga_historica
    paso "D.1 El consumidor muere a mitad de lote" experimentos.d1_consumidor_muere
    paso "D.2 Redis se reinicia (AOF vs sin persistencia)" experimentos.d2_reinicio_redis
    paso "B.2 Hora en vivo (productor 20x + ingesta + tablero)" pipeline.vivo
    verificar
    paso "C. Preguntas P1..P6" preguntas.correr_todas
    ;;
  pipeline)
    paso "B.1 Carga historica (864.000 eventos -> HBase)" pipeline.carga_historica
    paso "B.2 Hora en vivo (productor 20x + ingesta + tablero)" pipeline.vivo
    verificar
    ;;
  fallas)
    paso "D.1 El consumidor muere a mitad de lote" experimentos.d1_consumidor_muere
    paso "D.2 Redis se reinicia (AOF vs sin persistencia)" experimentos.d2_reinicio_redis
    echo "OJO: D.2 reinicia Redis; para volver a tener el tablero de la hora en vivo: ./run.sh pipeline"
    ;;
  preguntas) paso "C. Preguntas P1..P6" preguntas.correr_todas ;;
  verificar) verificar ;;
  *) echo "uso: ./run.sh [todo|pipeline|fallas|preguntas|verificar]"; exit 2 ;;
esac

echo
echo "Listo en $(( $(date +%s) - INICIO )) s. Resultados en resultados/*.md"
