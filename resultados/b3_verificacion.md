# B.3 Verificacion

| chequeo | obtenido | esperado |  |
|---|---|---|---|
| generador hist: eventos | 864,000 | 864,000 | OK |
| generador hist: repetidos | 17,090 | 17,090 | OK |
| generador hist: claves | 846,906 | 846,906 | OK |
| generador vivo: eventos | 7,200 | 7,200 | OK |
| generador vivo: repetidos | 153 | 153 | OK |
| generador vivo: claves | 7,047 | 7,047 | OK |
| generador total: eventos | 871,200 | 871,200 | OK |
| generador total: repetidos | 17,243 | 17,243 | OK |
| generador total: claves | 853,953 | 853,953 | OK |
| eventos producidos (contador del productor) | 7,200 | 7,200 | OK |
| entries-added del stream | 7,200 | 7,200 | OK |
| grupo ingesta: entradas procesadas (XACK) | 7,200 | 7,200 | OK |
| grupo ingesta: entries-read | 7,200 | 7,200 | OK |
| grupo ingesta: XPENDING | 0 | 0 | OK |
| grupo tablero: entradas procesadas (XACK) | 7,200 | 7,200 | OK |
| grupo tablero: entries-read | 7,200 | 7,200 | OK |
| grupo tablero: XPENDING | 0 | 0 | OK |
| filas HBase lecturas | 853,953 | 853,953 | OK |
| filas HBase lecturas_tipo | 853,953 | 853,953 | OK |
| tablero: ultimo:<est> correctos (por medicion) | 16/16 | 16/16 | OK |
| tablero: ranking:flujo | OK | OK | OK |
| tablero: ventana:<est> = ultimos 10 min de medicion | 8/8 | 8/8 | OK |
| tablero: catalogo tipo:<tipo> | OK | OK | OK |

Todo calza. HBase tiene 853.953 filas en cada tabla = 846.906 claves del historico + 7.047 de la hora en vivo. La diferencia con los 871.200 eventos son los 17.243 repetidos tal cual + 4 choques de clave (misma estacion y mismo ms, distinto valor) que quedan con el ultimo valor escrito.
