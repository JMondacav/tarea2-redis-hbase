"""Escritura de eventos a las dos tablas de HBase (la usan la carga historica y el consumidor ingesta)."""
from pipeline.comun import FAM, clave_lecturas, clave_tipo

COL_VALOR = f"{FAM}:valor".encode()
COL_TIPO = f"{FAM}:tipo".encode()
COL_EST = f"{FAM}:estacion".encode()


def put_evento(b_lect, b_tipo, ev):
    """Un evento = un put en cada tabla. La clave es idempotente: el mismo evento cae en la misma fila."""
    est, tipo, ts, valor = ev["estacion_id"], ev["tipo_sensor"], ev["timestamp"], str(ev["valor"]).encode()
    b_lect.put(clave_lecturas(est, ts), {COL_VALOR: valor, COL_TIPO: tipo.encode()})
    b_tipo.put(clave_tipo(tipo, ts, est), {COL_VALOR: valor, COL_EST: est.encode()})


def escribir_lote(conn, eventos, prefijo=""):
    """Escribe una lista de eventos en batch (un RPC por tabla). Si algo falla, lanza excepcion
    y el que llama NO hace XACK: el lote se reintenta y como la clave es idempotente no duplica."""
    t_lect = conn.table(f"{prefijo}lecturas")
    t_tipo = conn.table(f"{prefijo}lecturas_tipo")
    b_lect = t_lect.batch()
    b_tipo = t_tipo.batch()
    for ev in eventos:
        put_evento(b_lect, b_tipo, ev)
    b_lect.send()
    b_tipo.send()
