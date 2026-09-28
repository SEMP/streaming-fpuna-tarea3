"""Pruebas propias, además de la suite provista por la cátedra.

El notebook las pide explícitamente en su sección 3: «Agregá pruebas con `TestPipeline` y al
menos una prueba temporal con `TestStream` que evidencie un resultado late aceptado».

Van en un archivo aparte a propósito: `test_assignment.py` es de la cátedra y conviene que se
vea cuál es cuál.

Dos de estas fijan defectos que existieron de verdad y se corrigieron, para que no vuelvan:
el oráculo dependía del orden del archivo, y el pipeline no deduplicaba.
"""

from __future__ import annotations

import json
import random
from datetime import datetime
from pathlib import Path
from typing import Any

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as FlujoDePrueba
from apache_beam.testing.util import assert_that, equal_to

DATA_PATH = Path(__file__).parents[1] / "data" / "payments.jsonl"
VENTANA = 60
LATENESS = 120


def load_events() -> list[dict[str, Any]]:
    return [
        json.loads(linea)
        for linea in DATA_PATH.read_text(encoding="utf-8").splitlines()
        if linea.strip()
    ]


def marca(reloj: str) -> float:
    """Segundos desde época, que es lo que `TestStream` usa para ubicar un evento."""
    return datetime.fromisoformat(f"2026-07-24T{reloj}+00:00").timestamp()


def pago(event_id: str, event_time: str, amount: int, *, merchant: str = "m-a") -> dict:
    return {
        "event_id": event_id,
        "merchant_id": merchant,
        "event_time": f"2026-07-24T{event_time}Z",
        "arrival_time": f"2026-07-24T{event_time}Z",
        "amount": amount,
        "status": "CONFIRMED",
    }


def opciones_streaming() -> PipelineOptions:
    opciones = PipelineOptions()
    opciones.view_as(StandardOptions).streaming = True
    return opciones


# ------------------------------------------------------------- tiempo, con TestStream


def test_un_tardio_dentro_de_la_lateness_corrige_la_ventana(solution):
    """**La prueba temporal que pide el notebook: un late aceptado.**

    Es la única forma determinista de probar esto. El comportamiento tardío depende de dónde
    está el watermark, y con un reloj real habría que esperar y el resultado dependería de la
    máquina; `TestStream` deja decidir cuándo llega cada evento y hasta dónde avanzó el
    watermark.

    Llegan dos pagos de la ventana [13:00, 13:01), el watermark la cierra —se emite el pane
    on-time con 30— y recién entonces llega un tercero que pertenece a esa misma ventana. Como
    entra dentro de los 120 s de lateness, **se acepta y corrige**: aparece un segundo pane
    con 45.
    """
    flujo = (
        FlujoDePrueba()
        .advance_watermark_to(marca("13:00:00"))
        .add_elements([("m-a", 10)], event_timestamp=marca("13:00:05"))
        .add_elements([("m-a", 20)], event_timestamp=marca("13:00:42"))
        .advance_watermark_to(marca("13:01:30"))   # cierra la ventana: pane on-time
        .add_elements([("m-a", 15)], event_timestamp=marca("13:00:50"))  # tardío
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=opciones_streaming()) as pipeline:
        totales = (
            pipeline
            | flujo
            | solution.build_trigger_policy(
                window_seconds=VENTANA, allowed_lateness_seconds=LATENESS
            )
            | beam.CombinePerKey(sum)
            | beam.Map(lambda par: par[1])
        )
        # Dos panes: el on-time y la corrección. Con ACCUMULATING el segundo trae el total
        # completo —45, no el delta de 15—, que es lo que permite que el destino haga upsert.
        assert_that(totales, equal_to([30, 45]))


def test_un_tardio_fuera_de_la_lateness_no_corrige_nada(solution):
    """El otro lado de la misma decisión: pasado el horizonte, la ventana ya no acepta.

    El mismo flujo de arriba, pero el watermark avanza más allá de `fin + 120 s` antes de que
    llegue el tardío. La ventana ya fue recolectada, así que el evento no produce un pane
    nuevo y el total queda en 30.
    """
    flujo = (
        FlujoDePrueba()
        .advance_watermark_to(marca("13:00:00"))
        .add_elements([("m-a", 10)], event_timestamp=marca("13:00:05"))
        .add_elements([("m-a", 20)], event_timestamp=marca("13:00:42"))
        .advance_watermark_to(marca("13:05:00"))   # muy por encima de 13:01 + 120 s
        .add_elements([("m-a", 15)], event_timestamp=marca("13:00:50"))
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=opciones_streaming()) as pipeline:
        totales = (
            pipeline
            | flujo
            | solution.build_trigger_policy(
                window_seconds=VENTANA, allowed_lateness_seconds=LATENESS
            )
            | beam.CombinePerKey(sum)
            | beam.Map(lambda par: par[1])
        )
        assert_that(totales, equal_to([30]))


def test_el_desorden_no_es_lo_mismo_que_el_atraso(solution):
    """Un evento puede llegar fuera de orden y **no** ser tardío.

    El segundo pago tiene un `event_time` anterior al del primero, pero llega mientras la
    ventana sigue abierta: entra en la agregación normal y no hay corrección que hacer. Un
    solo pane con el total completo.
    """
    flujo = (
        FlujoDePrueba()
        .advance_watermark_to(marca("13:00:00"))
        .add_elements([("m-a", 10)], event_timestamp=marca("13:00:42"))
        .add_elements([("m-a", 20)], event_timestamp=marca("13:00:05"))  # anterior
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=opciones_streaming()) as pipeline:
        totales = (
            pipeline
            | flujo
            | solution.build_trigger_policy(
                window_seconds=VENTANA, allowed_lateness_seconds=LATENESS
            )
            | beam.CombinePerKey(sum)
            | beam.Map(lambda par: par[1])
        )
        assert_that(totales, equal_to([30]))


# ------------------------------------------------------ el pipeline, con TestPipeline


def test_el_pipeline_no_cuenta_dos_veces_un_duplicado(solution):
    """**Regresión.** El pipeline no deduplicaba: el mismo pago entraba dos veces en el total.

    Con el dataset real, `m-verde` en [13:00, 13:01) daba 160.000 en lugar de 80.000. La
    deduplicación va **antes** de sumar, y esta prueba lo fija.
    """
    duplicado = pago("p-1", "13:00:05", 10)
    with BeamTestPipeline() as pipeline:
        salida = solution.build_windowed_totals_pipeline(
            pipeline, [duplicado, dict(duplicado)], window_seconds=VENTANA
        )
        assert_that(salida | beam.Map(lambda fila: fila["total"]), equal_to([10]))


def test_dos_comercios_pueden_repetir_el_mismo_event_id(solution):
    """El estado de la deduplicación es **por clave**, así que no se interfieren.

    Es la razón por la que hay que clavear por `merchant_id` antes del `ParDo`: si el estado
    fuera global, el pago del segundo comercio desaparecería.
    """
    eventos = [
        pago("compartido", "13:00:05", 10, merchant="m-a"),
        pago("compartido", "13:00:06", 20, merchant="m-b"),
    ]
    with BeamTestPipeline() as pipeline:
        salida = solution.build_windowed_totals_pipeline(
            pipeline, eventos, window_seconds=VENTANA
        )
        assert_that(
            salida | beam.Map(lambda fila: (fila["merchant_id"], fila["total"])),
            equal_to([("m-a", 10), ("m-b", 20)]),
        )


def test_el_pipeline_ventanea_por_tiempo_de_evento_y_no_de_llegada(solution):
    """Dos pagos separados por más de un minuto **en tiempo de evento** caen en ventanas
    distintas, aunque lleguen juntos."""
    eventos = [pago("p-1", "13:00:30", 10), pago("p-2", "13:01:30", 20)]
    with BeamTestPipeline() as pipeline:
        salida = solution.build_windowed_totals_pipeline(
            pipeline, eventos, window_seconds=VENTANA
        )
        assert_that(
            salida | beam.Map(lambda f: (f["window_start"][11:16], f["total"])),
            equal_to([("13:00", 10), ("13:01", 20)]),
        )


# ------------------------------------------------------------------ el oráculo, puro


def test_el_oraculo_no_depende_del_orden_del_archivo(solution):
    """**Regresión.** Recorría el archivo tal como venía, no en orden de llegada.

    El total daba igual, pero la auditoría se daba vuelta: la copia tardía de un pago quedaba
    marcada como original y la original como duplicado. Se recorre ordenando por
    `arrival_time`, así que barajar el archivo no cambia nada.
    """
    eventos = load_events()
    esperado = solution.summarize_payments(eventos)

    for semilla in range(25):
        barajado = random.Random(semilla).sample(eventos, len(eventos))
        assert solution.summarize_payments(barajado) == esperado


def test_el_atraso_no_se_trunca_a_entero(solution):
    """**Regresión.** Con `int()`, 120,9 s contaba como 120 y el evento se aceptaba.

    El pago llega 120,9 s después de su medición, apenas por encima del horizonte. Tiene que
    quedar fuera.
    """
    evento = {
        "event_id": "p-limite",
        "merchant_id": "m-a",
        "event_time": "2026-07-24T13:00:00Z",
        "arrival_time": "2026-07-24T13:02:00.900Z",
        "amount": 10,
        "status": "CONFIRMED",
    }
    _, auditoria = solution.summarize_payments(
        [evento], allowed_lateness_seconds=LATENESS
    )
    assert auditoria[0]["too_late"] is True
    assert auditoria[0]["reason"] == "too_late"


def test_sin_deduplicar_el_total_cambia(solution):
    """El interruptor `deduplicate` existe para poder mostrar el contraste, y acá se mide.

    Con el dataset real hay una copia de `p-002`: deduplicando, `m-verde` suma 80.000; sin
    deduplicar, 160.000. Es el doble sobre un dato que se factura.
    """
    clave = ("m-verde", "2026-07-24T13:00:00+00:00")

    def total(totales):
        return next(
            fila["total"]
            for fila in totales
            if (fila["merchant_id"], fila["window_start"]) == clave
        )

    con, _ = solution.summarize_payments(load_events(), deduplicate=True)
    sin, _ = solution.summarize_payments(load_events(), deduplicate=False)
    assert total(con) == 80_000
    assert total(sin) == 160_000


# --------------------------------------------------------------- efectos externos


def test_la_clave_rechaza_el_separador(solution):
    """Con `|` libre dentro de las partes, dos celdas distintas producen la misma clave.

    `"a|b" + "c"` y `"a" + "b|c"` colapsan en `a|b|c`, y una pisaría a la otra en el destino.
    """
    with pytest.raises(ValueError, match=r"separador"):
        solution.make_idempotency_key(
            {"merchant_id": "m|raro", "window_start": "2026-07-24T13:00:00+00:00"}
        )


def test_reintentar_en_modo_append_si_duplica(solution):
    """El contraste que da sentido a la clave idempotente: sin upsert, N intentos, N filas."""
    resultados = [
        {
            "merchant_id": "m-a",
            "window_start": "2026-07-24T13:00:00+00:00",
            "window_end": "2026-07-24T13:01:00+00:00",
            "total": 30,
        }
    ]
    idempotente, _ = solution.simulate_sink_retries(resultados, attempts=3, idempotent=True)
    append, _ = solution.simulate_sink_retries(resultados, attempts=3, idempotent=False)

    assert len(idempotente) == 1
    assert len(append) == 3
