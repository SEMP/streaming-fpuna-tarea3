#!/usr/bin/env bash
# Captura una corrida completa en un solo archivo.
#
# Es uno de los cinco entregables de la tarea (clase 6, lámina 83). La idea no es probar
# que anda —para eso está el repositorio, que se puede correr— sino dejar una corrida
# fechada y con su commit, para poder comparar contra ella.
#
#     ./evidencia/generar-evidencia.sh
#
# Solo necesita uv. La variante con Docker está en el README.

set -uo pipefail
cd "$(dirname "$0")/.."

SALIDA="evidencia/evidencia-ejecucion.txt"
: > "$SALIDA"

titulo() {
  { echo; echo "════════════════════════════════════════════════════════════════════"
    echo " $1"
    echo "════════════════════════════════════════════════════════════════════"; } | tee -a "$SALIDA"
}

correr() {
  # El comando se registra antes de su salida: sin eso la evidencia no dice qué se ejecutó.
  { echo; echo "\$ $*"; } | tee -a "$SALIDA"
  "$@" 2>&1 | tee -a "$SALIDA"
  echo "  → código de salida: ${PIPESTATUS[0]}" | tee -a "$SALIDA"
}

{
  echo "Evidencia de ejecución — Tarea 3: estado, duplicados e idempotencia"
  echo "Generada por evidencia/generar-evidencia.sh"
  echo "Commit: $(git rev-parse --short HEAD 2>/dev/null || echo 'sin git')"
  echo "Fecha:  $(date --iso-8601=seconds)"
} | tee -a "$SALIDA"

titulo "1. Entorno"
correr uname -srm
correr uv run python --version
correr uv run python -c "import apache_beam; print('apache-beam', apache_beam.__version__)"

titulo "2. Dependencias, desde el lockfile"
# --frozen falla si el lockfile no coincide con pyproject.toml: es lo que hace verificable
# la reproducibilidad en lugar de afirmarla.
correr uv sync --frozen

titulo "3. Suite completa"
correr uv run pytest -q

titulo "4. Detalle por prueba"
correr uv run pytest -v --no-header

titulo "5. Las siete pruebas mínimas que pide la clase 6"
{
  echo
  printf '  %-26s %s\n' "duplicado" "test_duplicate_does_not_change_total"
  printf '  %-26s %s\n' "" "test_el_pipeline_no_cuenta_dos_veces_un_duplicado"
  printf '  %-26s %s\n' "claves aisladas" "test_stateful_dofn_keeps_keys_isolated"
  printf '  %-26s %s\n' "" "test_dos_comercios_pueden_repetir_el_mismo_event_id"
  printf '  %-26s %s\n' "fuera de orden" "test_out_of_order_event_uses_its_event_time_window"
  printf '  %-26s %s\n' "" "test_el_desorden_no_es_lo_mismo_que_el_atraso"
  printf '  %-26s %s\n' "late aceptado" "test_late_event_within_tolerance_is_a_revision"
  printf '  %-26s %s\n' "" "test_un_tardio_dentro_de_la_lateness_corrige_la_ventana"
  printf '  %-26s %s\n' "demasiado tardío" "test_event_beyond_lateness_is_audited"
  printf '  %-26s %s\n' "" "test_un_tardio_fuera_de_la_lateness_no_corrige_nada"
  printf '  %-26s %s\n' "timer de limpieza" "test_timer_handler_clears_state"
  printf '  %-26s %s\n' "escritura repetida" "test_retries_converge_to_one_materialized_entity"
  printf '  %-26s %s\n' "" "test_reintentar_en_modo_append_si_duplica"
} | tee -a "$SALIDA"

titulo "6. Linter"
correr uv run ruff check .

titulo "7. El notebook corriendo de punta a punta"
# `marimo export` ejecuta todas las celdas, asi que lo que sigue es la salida real del
# notebook: si una celda fallara, esto no terminaria en cero.
correr uv run marimo export html notebook.py -o /dev/null

titulo "Fin"
echo "  Evidencia en $SALIDA · $(wc -l < "$SALIDA") líneas" | tee -a "$SALIDA"
