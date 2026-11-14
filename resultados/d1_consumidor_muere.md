# D.1 El consumidor muere a mitad de lote

Stream propio `sensores:t2:d1` con los 7.200 eventos de la hora en vivo, tablas `d1_lecturas` y `d1_lecturas_tipo`
(recien creadas, VERSIONS => 3). `ingesta-1` lee de a 200 y en el lote 5 hace `os._exit(1)` **despues del
put y antes del XACK** (codigo de salida: 1).

## 1. Que quedo en XPENDING

- Entradas confirmadas antes de morir: 800 (lotes 1 a 4)
- `XPENDING sensores:t2:d1 ingesta` -> **200 pendientes**, de `1794626149004-2` a `1794626149090-1`,
  consumidores: [('ingesta-1', 200)]
- Filas en d1_lecturas en ese momento: 981 (el put del lote 5 ya estaba hecho)

Primeras pendientes (`XPENDING ... - + 3`):

| id | consumidor | ms sin confirmar | veces entregada |
|---|---|---|---|
| 1794626149004-2 | ingesta-1 | 27 | 1 |
| 1794626149005-0 | ingesta-1 | 27 | 1 |
| 1794626149005-1 | ingesta-1 | 27 | 1 |

## 2. Otro consumidor las recupera

`ingesta-2` arranca con `XAUTOCLAIM sensores:t2:d1 ingesta ingesta-2 2000 0-0 COUNT 200` (toma lo que lleva > 2 s sin
confirmar) y despues sigue leyendo con `XREADGROUP ... >` hasta el final:

```
[ingesta/ingesta-2] XAUTOCLAIM: rescate 200 entradas de otro consumidor
```

## 3. HBase al final

- `XPENDING` al final: **0**
- Filas: lecturas = **7,047**, lecturas_tipo = **7,047** (control hora en vivo: 7,047) ->
  calza exacto, ni faltan ni sobran

## 4. Por que no duplica, y que quedo en la celda

La clave es `estacion#ts_medicion`: el reproceso escribe exactamente las mismas filas, entonces no aparecen filas
nuevas, solo una version mas de cada celda. `cells(fila, 'd:valor', versions=3, include_timestamp=True)`:

| evento | fila | versiones | contenido (valor @ timestamp de escritura en ms) |
|---|---|---|---|
| lote 5 (reprocesado) | SPP-002#2026-11-07T00:06:40.000-03:00 | 2 | valor=90.0 @ 1794626155645; valor=90.0 @ 1794626152650 |
| lote 1 (normal) | CC-003#2026-11-07T00:00:00.000-03:00 | 1 | valor=53.9 @ 1794626152536 |

La fila del lote reprocesado tiene 2 versiones con el **mismo valor** (el put de ingesta-1 y el de ingesta-2, con
distinto timestamp de escritura); una lectura normal devuelve solo la mas nueva, asi que para quien consulta es lo
mismo. Con VERSIONS => 3 las versiones extra no crecen sin limite, y en la compactacion se descartan las que sobran.

## Contraprueba: XACK antes del put

Igual, pero `ingesta-1` hace el XACK primero y muere antes del put (stream `sensores:t2:d1x`, tablas `d1x_*`).
- XPENDING despues de morir: **0** -> no hay nada que rescatar, Redis cree que ya se proceso.
- Filas al final en d1x_lecturas: **6,851** de 7,047 -> **se perdieron 196** claves
  (las 196 claves del lote 5 que no se repiten en otro lote).
Por eso el XACK va despues del put.
