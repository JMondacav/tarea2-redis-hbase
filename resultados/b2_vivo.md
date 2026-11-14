# B.2 Hora en vivo

- Productor: **7,200 eventos en 180.0 s** (40.0 ev/s, objetivo 40 ev/s = 20x)
- Los consumidores terminaron 0.8 s despues del ultimo XADD
- Stream `sensores:t2`: XLEN 7,200 - entries-added 7,200 - MAXLEN ~10,000

| grupo | entries-read | pending | lag final | atraso maximo observado (entradas) |
|---|---|---|---|---|
| ingesta | 7200 | 0 | 0 | 100 |
| tablero | 7200 | 0 | 0 | 1 |

El atraso maximo es lo que el MAXLEN tiene que aguantar: si el atraso llegara a MAXLEN, el recorte
borraria entradas que el grupo aun no lee.
