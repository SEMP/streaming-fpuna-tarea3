# Tarea 3 — Beam avanzado

Proyecto base autocontenido para la asignatura **Streaming de datos y sus
aplicaciones**. La tarea consiste en completar un pipeline de pagos con tiempo
de evento, ventanas, estado por clave y una salida idempotente.

El repositorio es deliberadamente un esqueleto: `notebook.py` contiene la
consigna, contratos y funciones sin implementación. No incluye la solución.

## Objetivo

Producir totales confirmados por comercio y minuto:

- usando `event_time`, no el tiempo de llegada;
- tolerando hasta 120 segundos de atraso;
- descartando estados distintos de `CONFIRMED`;
- deduplicando `event_id` dentro de cada comercio;
- conservando metadatos de ventana y pane;
- materializando la salida mediante una clave idempotente.

## Ejecutar con Docker

Desde este directorio:

```bash
docker compose up --build notebook
```

Abrir <http://localhost:2718>. Docker inicia Marimo en modo editor porque la
tarea requiere completar las celdas de código. Los cambios en `notebook.py` se
guardan en el directorio local.

El editor usa `--no-token` para simplificar el trabajo en `localhost`; no debe
exponerse directamente a una red pública.

## Ejecutar con uv

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

## Trabajar con tests

```bash
uv run pytest
```

Los tests se entregan deliberadamente en rojo: las funciones del notebook
lanzan `NotImplementedError`. El objetivo es implementar las celdas hasta
obtener una suite completamente verde.

Los tests cargan las funciones directamente desde `notebook.py`; no hay que
copiar la solución a otro módulo.

Para validar además estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Dentro del contenedor también se puede ejecutar:

```bash
docker compose exec notebook uv run pytest
```

## Entrega

Entregar un repositorio propio que incluya:

- `notebook.py` con todas las funciones implementadas;
- evidencia de ejecución del pipeline;
- todas las pruebas provistas para desorden, duplicados, atraso y reintentos
  ejecutadas y aprobadas;
- un README breve con decisiones y trade-offs;
- instrucciones reproducibles con Docker o `uv`.

No modificar `data/payments.jsonl`; puede agregarse un conjunto de datos
adicional para las pruebas.

---

# Resolución

Implementado por **Sergio Morel**, septiembre de 2026.

## Estado de la suite

```
12 passed, 1 failed
```

La que falla **no se puede satisfacer con Apache Beam 2.74.0**, que es la versión que fija
el `pyproject.toml` de este mismo repositorio. El detalle y la evidencia están más abajo.

```bash
uv sync
uv run pytest -q
```

## Qué se implementó

| TODO | Qué resuelve |
|---|---|
| 1 · `parse_utc` | ISO-8601 → `datetime` **timezone-aware** en UTC. Acepta **cualquier offset explícito**, no solo `Z`, y rechaza un timestamp sin zona: un naive se interpretaría en la zona del proceso, y entonces la ventana asignada dependería de en qué máquina corre el pipeline |
| 2 · `assign_fixed_window` | Límites `[inicio, fin)` alineados **a la época**, no al primer evento, para que la misma ventana tenga los mismos límites entre comercios y entre corridas |
| 3 · `summarize_payments` | El oráculo en Python puro: totales y auditoría con el motivo de cada decisión |
| 4 · `build_windowed_totals_pipeline` | `Create` → `Filter` → `TimestampedValue` → `WindowInto` → clave por comercio → `CombinePerKey` → límites vía `WindowParam` |
| 5 · `DeduplicatePayments.process` | `SetStateSpec` **por clave**: dos comercios pueden repetir un `event_id` sin interferirse |
| 5b · `.expire` | Timer de *event time* que limpia el estado al vencer la ventana más la lateness |
| 6 · `build_trigger_policy` | `AfterWatermark` con pane temprano por tiempo de procesamiento, revisiones tardías y modo **acumulativo** |
| 7 · `make_idempotency_key` | `merchant_id|window_start`: identifica la **celda** del resultado, no el intento de escritura |
| 8 · `simulate_sink_retries` | Contrasta *upsert* contra *append*: el mismo reintento deja una fila o dos |

## Una comprobación agregada al notebook

El dataset trae todos sus timestamps en `Z`, así que la conversión de husos no queda
ejercitada por ninguna prueba. Se agregó una celda que la muestra, porque es la propiedad de
la que depende que la ventana sea estable:

```
entrada                          → en UTC                     → ventana
  2026-07-24T13:00:05Z           2026-07-24T13:00:05+00:00  [13:00, 13:01)
  2026-07-24T10:00:05-03:00      2026-07-24T13:00:05+00:00  [13:00, 13:01)
  2026-07-24T15:00:05+02:00      2026-07-24T13:00:05+00:00  [13:00, 13:01)
  2026-07-24T22:00:05+09:00      2026-07-24T13:00:05+00:00  [13:00, 13:01)
```

Los cuatro son **el mismo instante** escrito de cuatro maneras, y caen en la misma ventana.
`astimezone` no mueve el momento: solo cambia cómo se escribe. Si la función devolviera el
timestamp tal como vino, el mismo pago caería en ventanas distintas según cómo lo hubiera
expresado el emisor.

La celda muestra también los tres casos que se rechazan, incluido el más importante: un
timestamp **sin zona horaria**.

## Decisiones que el enunciado dejaba abiertas

**Orden de evaluación en la auditoría:** primero el estado (`not_confirmed`), después la
tolerancia (`too_late`) y al final la duplicación (`duplicate`). Un evento fuera de
tolerancia **no se registra como visto**: nunca entró, así que un reintento posterior del
mismo `event_id` debe volver a reportarse como fuera de tolerancia y no como duplicado.

**Qué es una revisión:** un evento **aceptado** cuyo `arrival_time` es posterior al cierre
de su ventana. Es decir, un total que alguien ya pudo haber leído y que cambia. Los eventos
rechazados no son revisiones, porque no modifican ningún total.

**Por qué el modo acumulativo:** cada pane trae el total de la ventana y no el delta, así el
consumidor hace *upsert* por clave sin necesidad de recordar qué panes ya procesó. Con modo
descartante habría que sumar, y entonces reprocesar un pane infla el resultado — la
deduplicación se mudaría aguas abajo.

**Por qué el estado necesita expiración:** la entrada de un pipeline de streaming es **no
acotada**, así que el conjunto de `event_id` vistos crece sin límite si nadie lo limpia. El
timer se programa contra el fin de la ventana **más la lateness permitida**, no contra el
fin de la ventana: si expirara antes, un tardío legítimo volvería a parecer nuevo y se
contaría dos veces — justo lo que la deduplicación existe para evitar.

## ⚠️ Una prueba de la suite es insatisfacible en Beam 2.74.0

`test_trigger_policy_has_lateness_and_accumulating_panes` incluye estas dos aserciones:

```python
assert policy.windowing.windowfn.size.seconds == 60
assert policy.windowing.allowed_lateness.seconds == 120
```

Ambas leen un atributo `.seconds` sobre objetos `Duration`, **y `Duration` no lo tiene**:

```python
>>> from apache_beam.utils.timestamp import Duration, Timestamp
>>> [a for a in dir(Duration) if not a.startswith("_")]
['from_proto', 'of', 'to_proto']
>>> [a for a in dir(Timestamp) if not a.startswith("_")]
[..., 'seconds', ...]          # Timestamp sí lo tiene; Duration no
```

No es una elección de implementación que se pueda evitar. Beam convierte los dos valores
sin dar alternativa:

- `Windowing.__init__` hace `self.allowed_lateness = Duration.of(allowed_lateness)`
  (`apache_beam/transforms/core.py`);
- `FixedWindows.__init__` hace `self.size = Duration.of(size)`
  (`apache_beam/transforms/window.py`), y además **rechaza** un `timedelta`, que sí tendría
  `.seconds`.

Las **otras dos aserciones de esa misma prueba sí pasan**, y son las que verifican el
comportamiento: el modo es `ACCUMULATING` y el trigger es `AfterWatermark`. Lo que no se
puede leer es el tamaño y la lateness *por esa vía*. Que los valores son correctos se
comprueba así:

```python
>>> policy.windowing.windowfn.size          # Duration(60)
>>> policy.windowing.allowed_lateness       # Duration(120)
```

**Existe un apaño y se decidió no usarlo.** `Duration.of()` devuelve la instancia tal cual
si ya es un `Duration`, así que una subclase con una propiedad `seconds` sobreviviría hasta
`windowing` y la prueba pasaría. Se descartó porque sería código cuyo único propósito es
satisfacer una suposición incorrecta de la prueba, sin cambiar en nada el comportamiento del
pipeline — y ocultaría el hallazgo en lugar de reportarlo.

Queda a consulta con la cátedra si la prueba se desarrolló contra otra versión de Beam.
