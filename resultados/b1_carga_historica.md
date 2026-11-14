# B.1 Carga historica

- Eventos leidos del generador: **864,000**
- Puts enviados: **1,728,000** (864,000 por tabla), en batch de 5,000 filas por RPC
- Tiempo total de carga: **23.8 s** -> **36,307 eventos/s** (72,615 puts/s)
- Conteo con `FirstKeyOnlyFilter() AND KeyOnlyFilter()`: 8.5 s

| tabla | filas | esperado (claves distintas historico) | ok |
|---|---|---|---|
| lecturas | 846,906 | 846,906 | si |
| lecturas_tipo | 846,906 | 846,906 | si |

Las 17,094 filas que "faltan" son los 17.090 repetidos tal cual + 4 choques de clave
(misma estacion y mismo ms con distinto valor): caen en la misma fila, por eso no suman.
