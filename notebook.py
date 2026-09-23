import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC.

        El resultado es siempre timezone-aware. Un naive sería peor que un error:
        se interpretaría en la zona del proceso y la ventana asignada dependeria
        de en que maquina corre el pipeline.
        """
        from datetime import UTC

        if not isinstance(raw_value, str) or not raw_value:
            raise ValueError(f"timestamp invalido: {raw_value!r}")

        # fromisoformat no acepta la Z de Zulu en Python < 3.11; normalizarla
        # deja un offset explicito que si entiende.
        normalizado = raw_value.strip()
        if normalizado.endswith(("Z", "z")):
            normalizado = normalizado[:-1] + "+00:00"

        try:
            momento = datetime.fromisoformat(normalizado)
        except ValueError as error:
            raise ValueError(f"timestamp ISO-8601 invalido: {raw_value!r}") from error

        if momento.tzinfo is None:
            raise ValueError(
                f"timestamp sin zona horaria: {raw_value!r}. "
                "Se exige offset explicito para que la ventana no dependa del entorno."
            )

        return momento.astimezone(UTC)

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Comprobación: distintos husos, el mismo instante

    El dataset trae todos sus timestamps en `Z`, así que la conversión de husos no
    queda ejercitada por las pruebas. Vale la pena mostrarla, porque es la propiedad
    de la que depende que la ventana sea estable.

    `parse_utc` acepta **cualquier offset explícito** y devuelve siempre el mismo
    instante en UTC. `astimezone` no mueve el momento: solo cambia cómo se escribe.
    Por eso el mismo pago cae en la misma ventana aunque el emisor lo haya expresado
    en su hora local.

    Lo que **no** acepta es un timestamp sin zona: Python lo interpretaría en la zona
    del proceso, y entonces la ventana asignada dependería de en qué máquina corre el
    pipeline. Un error ruidoso es mejor que un resultado que cambia de servidor a
    servidor.
    """)
    return


@app.cell
def _(assign_fixed_window, parse_utc):
    _equivalentes = [
        "2026-07-24T13:00:05Z",
        "2026-07-24T10:00:05-03:00",
        "2026-07-24T15:00:05+02:00",
        "2026-07-24T22:00:05+09:00",
    ]

    print("entrada                          → en UTC                     → ventana")
    for _crudo in _equivalentes:
        _momento = parse_utc(_crudo)
        _inicio, _fin = assign_fixed_window(_momento, 60)
        print(
            f"  {_crudo:30} {_momento.isoformat()}  "
            f"[{_inicio:%H:%M}, {_fin:%H:%M})"
        )

    # La comprobación que importa: escritos distinto, son el mismo instante.
    assert len({parse_utc(_c) for _c in _equivalentes}) == 1
    print("\n  los cuatro son el mismo instante ✔")

    print("\nvalores que se rechazan:")
    for _malo in ["2026-07-24T13:00:05", "", "24/07/2026 13:00"]:
        try:
            parse_utc(_malo)
            print(f"  {_malo!r:24} aceptado (no debería)")
        except ValueError as _error:
            print(f"  {_malo!r:24} {_error}")
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los limites [inicio, fin) de la ventana fija.

        Las ventanas estan alineadas a la epoca, no al primer evento: asi la misma
        ventana tiene los mismos limites para todos los comercios y entre corridas
        distintas, que es lo que hace comparables los totales.
        """
        from datetime import UTC, timedelta

        if size_seconds <= 0:
            raise ValueError(f"el tamano de ventana debe ser positivo: {size_seconds}")
        if timestamp.tzinfo is None:
            raise ValueError("el timestamp debe ser timezone-aware")

        momento = timestamp.astimezone(UTC)
        epoca = datetime(1970, 1, 1, tzinfo=UTC)
        transcurrido = int((momento - epoca).total_seconds())
        inicio = epoca + timedelta(seconds=transcurrido - transcurrido % size_seconds)
        return inicio, inicio + timedelta(seconds=size_seconds)

    return


@app.cell
def _(Any, Iterable):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        # Oraculo en Python puro: el pipeline Beam tiene que coincidir con esto.
        # Tenerlo aparte permite discutir el contrato sin pelear con el runner.
        totales: dict[tuple[str, str], dict[str, Any]] = {}
        auditoria: list[dict[str, Any]] = []
        vistos: set[tuple[str, str]] = set()

        for evento in events:
            event_id = evento["event_id"]
            merchant_id = evento["merchant_id"]
            event_time = parse_utc(evento["event_time"])
            arrival_time = parse_utc(evento["arrival_time"])
            inicio, fin = assign_fixed_window(event_time, window_seconds)

            atraso = int((arrival_time - event_time).total_seconds())
            demasiado_tarde = atraso > allowed_lateness_seconds
            clave_dedup = (merchant_id, event_id)
            # La deduplicacion es POR COMERCIO: dos comercios distintos pueden usar
            # el mismo event_id sin pisarse.
            duplicado = deduplicate and clave_dedup in vistos

            if evento.get("status") != "CONFIRMED":
                aceptado, motivo = False, "not_confirmed"
            elif demasiado_tarde:
                # Se descarta antes de registrarlo como visto: nunca entro, asi que
                # un reintento posterior del mismo id no deberia reportarse como
                # duplicado sino, otra vez, como fuera de tolerancia.
                aceptado, motivo = False, "too_late"
            elif duplicado:
                aceptado, motivo = False, "duplicate"
            else:
                aceptado, motivo = True, "accepted"

            # Una revision es un evento aceptado que llego despues de que su ventana
            # cerro: el total que alguien ya vio cambia.
            revision = aceptado and arrival_time >= fin

            if aceptado:
                vistos.add(clave_dedup)
                clave = (merchant_id, inicio.isoformat())
                fila = totales.setdefault(
                    clave,
                    {
                        "merchant_id": merchant_id,
                        "window_start": inicio.isoformat(),
                        "window_end": fin.isoformat(),
                        "total": 0,
                    },
                )
                fila["total"] += evento["amount"]

            auditoria.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": atraso,
                    "duplicate": duplicado,
                    "too_late": demasiado_tarde,
                    "accepted": aceptado,
                    "revision": revision,
                    "reason": motivo,
                }
            )

        ordenados = sorted(
            totales.values(), key=lambda fila: (fila["window_start"], fila["merchant_id"])
        )
        return ordenados, auditoria

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        from datetime import UTC

        def con_timestamp(evento):
            """Reemplaza el timestamp de procesamiento por el del dominio.

            Sin esto Beam usaria el instante en que vio el elemento, y la ventana
            dependeria de cuando llego el dato y no de cuando ocurrio el pago.
            """
            return beam.window.TimestampedValue(
                evento, parse_utc(evento["event_time"]).timestamp()
            )

        def con_limites(par, ventana=beam.DoFn.WindowParam):
            """Recupera los limites de la ventana, que el agregado por si solo pierde."""
            merchant_id, total = par
            inicio = ventana.start.to_utc_datetime().replace(tzinfo=UTC)
            fin = ventana.end.to_utc_datetime().replace(tzinfo=UTC)
            return {
                "merchant_id": merchant_id,
                "window_start": inicio.isoformat(),
                "window_end": fin.isoformat(),
                "total": total,
            }

        return (
            pipeline
            | "Crear" >> beam.Create(events)
            | "SoloConfirmados" >> beam.Filter(lambda e: e.get("status") == "CONFIRMED")
            | "TiempoDeEvento" >> beam.Map(con_timestamp)
            | "Ventanear" >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "ClavePorComercio" >> beam.Map(lambda e: (e["merchant_id"], e["amount"]))
            | "Sumar" >> beam.CombinePerKey(sum)
            | "AgregarLimites" >> beam.Map(con_limites)
        )

    return


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120):
            # Hasta cuando se sigue recordando un event_id. Tiene que cubrir la
            # lateness permitida: si el estado se limpiara antes, un tardio
            # legitimo volveria a parecer nuevo y se contaria dos veces.
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparicion.

            El estado es **por clave**: Beam mantiene un SEEN_IDS separado para cada
            comercio, asi que dos comercios pueden usar el mismo event_id sin
            interferirse. Esa es la razon de clavear por merchant_id antes del ParDo.
            """
            _, payload = element
            event_id = payload["event_id"]

            if event_id in seen_ids.read():
                return  # ya lo vimos en esta clave: no se emite nada

            seen_ids.add(event_id)
            # El timer se programa contra el fin de la ventana mas la lateness. Sin
            # expiracion, el conjunto de ids vistos crece sin limite: en streaming la
            # entrada es no acotada, asi que "recordar todo" no es una opcion.
            expiry.set(window.end + self.allowed_lateness_seconds)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time.

            Se dispara una sola vez por clave y ventana, cuando el watermark pasa el
            fin de la ventana mas la lateness. En ese punto ya no puede llegar nada
            para esa ventana, asi que recordar sus ids no sirve de nada.
            """
            seen_ids.clear()

    return


@app.cell
def _(Any):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        from apache_beam.transforms import trigger

        return beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                # Vista temprana por tiempo de procesamiento: el tablero no queda en
                # blanco mientras la ventana se llena. Es un piso, no un total.
                early=trigger.AfterProcessingTime(window_seconds // 2 or 1),
                # Cada tardio dispara su propia correccion: son pocos y conviene
                # reflejarlos de inmediato en lugar de esperar un lote.
                late=trigger.AfterCount(1),
            ),
            allowed_lateness=allowed_lateness_seconds,
            # ACUMULATIVO: cada pane trae el total de la ventana, no el delta. El
            # consumidor hace upsert por clave y no necesita recordar que panes ya
            # proceso, lo que lo vuelve idempotente ante reintentos y replay.
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado logico.

        La clave identifica la **celda** del resultado, no el intento de escritura.
        Por eso recalcular una ventana produce la misma clave y reemplaza el valor
        anterior en lugar de agregar una fila nueva.
        """
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, multiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.

        El contraste es el punto: el pipeline no puede evitar reintentar —un timeout
        de escritura no dice si la escritura llego—, asi que la idempotencia tiene
        que estar en la **forma de la salida**, no en no reintentar.
        """
        if attempts < 1:
            raise ValueError(f"attempts debe ser al menos 1: {attempts}")

        por_clave: dict[str, dict[str, Any]] = {}
        agregados: list[dict[str, Any]] = []
        auditoria: list[dict[str, Any]] = []

        for resultado in results:
            clave = make_idempotency_key(resultado)
            for intento in range(1, attempts + 1):
                fila = {**resultado, "idempotency_key": clave}
                if idempotent:
                    por_clave[clave] = fila  # el segundo intento pisa al primero
                else:
                    agregados.append(fila)  # cada intento deja su propia fila
                auditoria.append(
                    {
                        "idempotency_key": clave,
                        "attempt": intento,
                        "operation": "UPSERT" if idempotent else "POST",
                        "materialized_rows": len(por_clave) if idempotent else len(agregados),
                    }
                )

        materializado = list(por_clave.values()) if idempotent else agregados
        return materializado, auditoria

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [ ] un duplicado no modifica el total;
    - [ ] claves distintas no comparten estado;
    - [ ] un evento fuera de orden cae en su ventana de evento;
    - [ ] un evento con atraso aceptado produce una revisión;
    - [ ] un evento demasiado tardío queda auditado;
    - [ ] dos escrituras del mismo resultado dejan una sola entidad;
    - [ ] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
