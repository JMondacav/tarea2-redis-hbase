# P4 Dos patrones de acceso - catalogo Redis + HBase

Rango [07:00, 09:00) del 05-nov, solo estaciones de calidad del aire (PM2.5).

(a) `SMEMBERS tipo:calidad_aire` en Redis -> 4 estaciones -> un `scan` por estacion en `lecturas`
    (`<est>#2026-11-05T07:00:00.000-03:00` .. `<est>#2026-11-05T09:00:00.000-03:00`).
(b) un solo `scan` en `lecturas_tipo` de `calidad_aire#2026-11-05T07:00:00.000-03:00` a `calidad_aire#2026-11-05T09:00:00.000-03:00`.

| estacion | lecturas | promedio PM2.5 | maximo | hora del maximo (1a vez) |
|---|---|---|---|---|
| CC-004 | 877 | 68.09 | 80.0 | 07:22:15.000 |
| CHI-001 | 904 | 62.18 | 80.0 | 07:07:14.500 |
| COR-001 | 890 | 22.78 | 52.3 | 08:18:32.500 |
| TAL-003 | 895 | 48.14 | 76.5 | 07:27:43.500 |

(a) y (b) dan exactamente lo mismo.

## Tiempos

- (a) catalogo Redis + 4 rangos: **mediana 25.64 ms** (5 corridas; la primera, aparte: 28.92 ms) - todas: [28.92, 23.43, 25.64, 30.94, 23.6, 26.39]
- (b) un scan en lecturas_tipo: **mediana 16.80 ms** (5 corridas; la primera, aparte: 19.20 ms) - todas: [19.2, 16.54, 16.2, 18.34, 16.8, 17.99]

## Lo que cuesta la segunda tabla

| tabla | filas | bytes en disco (despues de flush) |
|---|---|---|
| lecturas | 853,953 | 127,400,652 |
| lecturas_tipo | 853,953 | 152,368,565 |

- Escrituras: cada evento se escribe 2 veces. En la carga historica fueron 1.728.000 puts en vez de 864.000,
  y en vivo 14.400 en vez de 7.200.
- Disco: lecturas_tipo ocupa 1.20 veces lo de lecturas (la clave es mas larga
  porque lleva el nombre del tipo y la estacion) -> en total se paga ~2.2x.
- Si falla la segunda escritura: HBase no tiene transacciones entre tablas. En `ingesta` el lote se escribe en
  `lecturas` y despues en `lecturas_tipo`; si la segunda falla se lanza la excepcion, **no hay XACK**, el lote queda
  pendiente y se reintenta entero. La primera tabla recibe el mismo put otra vez (misma clave, no duplica) y al final las
  dos quedan iguales. Mientras tanto hay un rato en que `lecturas` tiene el evento y `lecturas_tipo` no.

**Interpretacion**: aqui la segunda tabla casi no gana nada: hay solo 4 estaciones de calidad del aire, asi que
(a) son 4 scans cortos y (b) uno solo, y los dos estan en el orden de los milisegundos. No valdria la pena
cuando hay pocas estaciones por tipo (como aqui) o la consulta por tipo es rara, porque se paga el doble de
escrituras y de disco siempre. Valdria la pena con miles de estaciones por tipo, donde (a) serian miles de RPC.
