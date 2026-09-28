# Tarea 3 — Beam avanzado

**Resolución de Sergio Morel**, sobre el proyecto base de la asignatura **Streaming de datos
y sus aplicaciones**. La tarea consiste en completar un pipeline de pagos con tiempo de
evento, ventanas, estado por clave y una salida idempotente.

El [proyecto base](https://github.com/rparrapy/streaming-fpuna-clase6-tarea) es un esqueleto:
`notebook.py` trae la consigna, los contratos y las funciones sin implementar. Ese contenido
está en el **primer commit** de este repositorio, `9543164`, así que el trabajo hecho se puede
ver con un diff:

```bash
git diff 9543164 HEAD -- notebook.py
```

Los ocho TODO están resueltos y la suite completa —24 pruebas— queda en verde.

> ### 📌 Nota para la cátedra
>
> **Una de las pruebas provistas no se puede satisfacer escribiendo el TODO**, y conviene
> avisarlo antes de que parezca un descuido.
> `test_trigger_policy_has_lateness_and_accumulating_panes` lee `.seconds` sobre objetos
> `Duration`, y en Apache Beam **2.74.0** —la versión que fija el `pyproject.toml` del propio
> proyecto base— `Duration` no expone ese atributo: lo tiene `Timestamp`. Beam convierte los
> dos valores con `Duration.of()` sin dar alternativa, así que ninguna forma de escribir
> `build_trigger_policy` hace pasar esas dos aserciones.
>
> Se resolvió con una subclase de `Duration` que agrega el accesor, documentada y sin efecto
> sobre el comportamiento del pipeline.
>
> **La consulta es si la prueba se escribió contra otra versión de Beam**, en cuyo caso
> conviene fijar esa versión en el proyecto base. El análisis completo, con la evidencia,
> está en [esta sección](#-una-prueba-de-la-suite-no-se-puede-satisfacer-escribiendo-el-todo).

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
24 passed
```

**13 son de la cátedra** (`tests/test_assignment.py`, sin tocar) y **11 son propias**
(`tests/test_propias.py`). El notebook las pide en su sección 3: «Agregá pruebas con
`TestPipeline` y al menos una prueba temporal con `TestStream` que evidencie un resultado
late aceptado».

Una de ellas **no se puede satisfacer escribiendo el TODO como pide el enunciado**: lee un
atributo que Apache Beam 2.74.0 no expone. Se resolvió con un adaptador mínimo y documentado,
y el hallazgo se reporta igual porque es información útil para la cátedra. El detalle y la
evidencia están más abajo.

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

## Evidencia de ejecución

```bash
./evidencia/generar-evidencia.sh
```

Deja [`evidencia/evidencia-ejecucion.txt`](evidencia/evidencia-ejecucion.txt) con una corrida
completa: entorno y versiones, `uv sync --frozen`, la suite entera, el detalle prueba por
prueba, el linter, y el notebook ejecutándose de punta a punta con `marimo export`. Cada paso
registra el comando y su código de salida, y el archivo lleva fecha y commit.

`--frozen` está a propósito: falla si el lockfile no coincide con `pyproject.toml`, así que la
reproducibilidad queda verificada y no solamente afirmada. Y `marimo export` corre todas las
celdas, así que si alguna fallara la corrida no terminaría en cero.

### Las siete pruebas mínimas de la clase 6

| Caso pedido | Dónde está |
|---|---|
| Duplicado | `test_duplicate_does_not_change_total` · `test_el_pipeline_no_cuenta_dos_veces_un_duplicado` |
| Claves aisladas | `test_stateful_dofn_keeps_keys_isolated` · `test_dos_comercios_pueden_repetir_el_mismo_event_id` |
| Evento fuera de orden | `test_out_of_order_event_uses_its_event_time_window` · `test_el_desorden_no_es_lo_mismo_que_el_atraso` |
| Late aceptado | `test_late_event_within_tolerance_is_a_revision` · `test_un_tardio_dentro_de_la_lateness_corrige_la_ventana` |
| Evento demasiado tardío | `test_event_beyond_lateness_is_audited` · `test_un_tardio_fuera_de_la_lateness_no_corrige_nada` |
| Timer de limpieza | `test_timer_handler_clears_state` |
| Escritura repetida | `test_retries_converge_to_one_materialized_entity` · `test_reintentar_en_modo_append_si_duplica` |

## Qué produce el dataset provisto

Lo pide el notebook en su sección 2. Con la configuración por defecto —ventana de 60 s,
lateness de 120 s, deduplicación activa:

| | |
|---|---|
| Eventos que entran | **9** |
| Aceptados | **5** |
| Rechazados | **4** — 2 `not_confirmed`, 1 `duplicate`, 1 `too_late` |
| De los aceptados, revisiones | **1** |
| Totales que se producen | **4** |

Los cuatro rechazos son **cuatro motivos distintos**, que es lo que hace útil al dataset.
Y la única revisión —la copia de `p-002`, que llega 83 s tarde— se **acepta** y aun así no
cambia el total, porque la deduplicación la reconoce: *aceptado* y *sumado* no son lo mismo.

## Las pruebas propias

Van en `tests/test_propias.py`, aparte de las de la cátedra, para que se vea cuál es cuál.

| Qué fija | Por qué está |
|---|---|
| **Un tardío dentro de la lateness corrige la ventana** | Es la prueba temporal que pide el notebook. Con `TestStream`, porque el comportamiento tardío depende de dónde está el watermark y con un reloj real el resultado dependería de la máquina |
| Un tardío **fuera** de la lateness no corrige nada | El otro lado de la misma decisión |
| Desorden **no** es lo mismo que atraso | Un evento puede llegar fuera de orden con la ventana todavía abierta |
| El pipeline no cuenta dos veces un duplicado | **Regresión**: no deduplicaba, y `m-verde` daba 160.000 en lugar de 80.000 |
| Dos comercios pueden repetir el mismo `event_id` | El estado es por clave |
| El pipeline ventanea por tiempo de evento | Que no se cuele el de llegada |
| El oráculo no depende del orden del archivo | **Regresión**: el total daba igual pero la auditoría se daba vuelta |
| El atraso no se trunca a entero | **Regresión**: 120,9 s contaba como 120 y se aceptaba |
| Sin deduplicar el total cambia | Mide el contraste: 80.000 contra 160.000 |
| La clave rechaza el separador | `"a\|b"+"c"` y `"a"+"b\|c"` colapsan en la misma clave |
| Reintentar en modo append duplica | El contraste que da sentido a la clave idempotente |

Tres de ellas fijan defectos que **existieron de verdad** y los encontró una revisión cruzada,
no una corrida. Están marcadas como regresión para que no vuelvan.

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

## ⚠️ Una prueba de la suite no se puede satisfacer escribiendo el TODO

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

### Cómo se resolvió

`Duration.of()` devuelve la instancia tal cual si ya es un `Duration`, así que una subclase
que exponga `seconds` sobrevive a `FixedWindows` y a `Windowing`, y la prueba pasa:

```python
class DuracionConSegundos(Duration):
    @property
    def seconds(self) -> int:
        return self.micros // 1_000_000
```

No cambia el comportamiento del pipeline: es el mismo valor con un accesor de más, el que
`Timestamp` ya tiene y `Duration` no.

**Se había decidido lo contrario, y se revirtió el 28/09.** El argumento original era que un
apaño para satisfacer una suposición de la prueba ocultaría el hallazgo. Pesaron más dos
cosas: la entrega pide la suite completa en verde, y dejar una prueba provista en rojo obliga
a quien corrige a leer el README para saber si es un defecto o una incompatibilidad. Reportar
el hallazgo y además pasar la prueba no son excluyentes — esta sección es el reporte.

### La consulta, concretamente

**¿La prueba se escribió contra otra versión de Beam?** Si es así, conviene fijar esa versión
en el `pyproject.toml` del proyecto base, porque hoy el repositorio base fija 2.74.0 y con esa
versión la prueba no puede pasar sin un adaptador como el de arriba.

Si en cambio la intención era que el alumno encontrara y resolviera la incompatibilidad,
entonces está resuelta, y esta sección es el reporte.
