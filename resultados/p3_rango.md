# P3 Un rango - HBase

(a) `scan(row_start=b'CC-001#2026-11-04T08:00:00.000-03:00', row_stop=b'CC-001#2026-11-04T10:00:00.000-03:00')` -> rango semiabierto [08:00, 10:00).
(b) `scan(filter="RowFilter(=, 'regexstring:^CC-001#2026-11-04T0[89]:')")` -> el filtro corre en el servidor pero igual hay que leer las 853,953 filas.

**891 lecturas** (por rango) / **891 lecturas** (tabla entera) -> mismo resultado.

| cuarto de hora | lecturas | promedio |
|---|---|---|
| 08:00 | 112 | 15.61 |
| 08:15 | 125 | 25.67 |
| 08:30 | 97 | 17.13 |
| 08:45 | 104 | 25.87 |
| 09:00 | 116 | 24.40 |
| 09:15 | 91 | 18.10 |
| 09:30 | 128 | 19.24 |
| 09:45 | 118 | 17.49 |

## Tiempos

- (a) scan por rango: **mediana 5.73 ms** (5 corridas; la primera, aparte: 6.17 ms) - todas: [6.17, 5.35, 7.07, 5.42, 6.39, 5.73]
- (b) tabla entera + filtro: **mediana 531.44 ms** (5 corridas; la primera, aparte: 518.59 ms) - todas: [518.59, 523.24, 526.77, 556.88, 531.44, 567.98]
- (b) es **93 veces** mas lento.

**Interpretacion**: el scan por rango va directo a la region de CC-001 y lee solo las ~891 filas que pide
(la clave estacion#ts las deja juntas y ordenadas); el filtro tiene que pasar por las 853,953 filas.
Con cinco años en vez de cinco dias (365 veces mas datos) el scan por rango tardaria casi lo mismo, porque
depende de cuantas filas devuelve y no del tamano de la tabla, mientras que el recorrido entero creceria ~365 veces
(de 0.5 s a ~3 min en esta maquina). En un cluster el recorrido se
repartiria entre RegionServers, pero seguiria siendo lineal en el tamano de la tabla.
