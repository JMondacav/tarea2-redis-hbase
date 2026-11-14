"""Consumidor del grupo `ingesta`: lee el stream en lotes y escribe en HBase (las dos tablas). XACK despues del put.

    python -m pipeline.ingesta --nombre ingesta-1
    python -m pipeline.ingesta --nombre ingesta-1 --morir-en-lote 5      # D.1
"""
import argparse

from pipeline.comun import GRUPO_INGESTA, STREAM, conectar_hbase
from pipeline.consumidor import Consumidor
from pipeline.hbase_io import escribir_lote


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nombre", default="ingesta-1")
    ap.add_argument("--stream", default=STREAM)
    ap.add_argument("--grupo", default=GRUPO_INGESTA)
    ap.add_argument("--prefijo-tablas", default="", help="para experimentos: escribe en <prefijo>lecturas")
    ap.add_argument("--lote", type=int, default=200)
    ap.add_argument("--min-idle-ms", type=int, default=10_000)
    ap.add_argument("--morir-en-lote", type=int, default=None)
    ap.add_argument("--ack-antes", action="store_true")
    ap.add_argument("--pausa", type=float, default=0.25, help="s de espera si el lote vino chico (junta lotes)")
    a = ap.parse_args()

    estado = {"conn": conectar_hbase()}

    def procesar(eventos):
        try:
            escribir_lote(estado["conn"], eventos, a.prefijo_tablas)
        except Exception:
            # la conexion Thrift puede quedar rota: se rehace y se relanza para que NO haya XACK
            try:
                estado["conn"].close()
            except Exception:  # noqa: BLE001
                pass
            estado["conn"] = conectar_hbase()
            raise

    Consumidor(a.grupo, a.nombre, procesar, stream=a.stream, lote=a.lote, min_idle_ms=a.min_idle_ms,
               morir_en_lote=a.morir_en_lote, ack_antes=a.ack_antes, pausa_lote_chico=a.pausa).correr()


if __name__ == "__main__":
    main()
