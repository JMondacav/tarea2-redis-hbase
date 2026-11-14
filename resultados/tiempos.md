# Tiempos (mediana de 5 corridas, la primera aparte)

Maquina: Windows 11 - AMD64 - 16 hilos - Python 3.12.10
(Redis 7.4 y HBase 2.5 standalone en Docker en la misma maquina)

| consulta | mediana (ms) | primera corrida (ms) |
|---|---|---|
| P1 Redis (16 ultimos + top5) | 0.81 | 9.08 |
| P1 HBase (16 scans inversos) | 36.00 | 37.13 |
| P2 Redis (8 ventanas) | 1.49 | 4.86 |
| P3 HBase scan por rango | 5.73 | 6.17 |
| P3 HBase tabla entera + filtro | 531.44 | 518.59 |
| P4 (a) catalogo Redis + 4 rangos en lecturas | 25.64 | 28.92 |
| P4 (b) 1 scan en lecturas_tipo | 16.80 | 19.20 |
| P5 p5_est_ts (16 rangos) | 55.72 | 70.36 |
| P5 p5_ts_est (1 rango) | 9.63 | 11.04 |
| P5 p5_salt (4 rangos) | 19.15 | 17.84 |
| P6 Redis (que) + HBase (cuando) | 160.09 | 186.06 |
