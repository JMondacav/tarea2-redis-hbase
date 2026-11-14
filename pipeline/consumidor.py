"""Base comun de los consumidores (consumer groups de Redis Streams).

Orden de cada lote: XREADGROUP -> procesar (put en HBase / actualizar tablero) -> XACK.
El XACK va DESPUES: si el proceso muere entre medio, las entradas quedan en XPENDING y otro
consumidor (o el mismo al volver) las reprocesa. Como todo lo que se escribe es idempotente,
reprocesar no duplica nada.
"""
import os
import time

import redis

from pipeline.comun import STREAM, conectar_redis


class Consumidor:
    def __init__(self, grupo, nombre, procesar, stream=STREAM, lote=200, min_idle_ms=10_000,
                 morir_en_lote=None, ack_antes=False, salir_al_terminar=True, pausa_lote_chico=0.0):
        self.grupo, self.nombre, self.procesar, self.stream = grupo, nombre, procesar, stream
        self.lote, self.min_idle_ms = lote, min_idle_ms
        self.morir_en_lote, self.ack_antes = morir_en_lote, ack_antes   # solo para el experimento D.1
        self.salir_al_terminar = salir_al_terminar
        # si el lote vino chico (el consumidor va al dia), esperar un poco para que el proximo junte mas:
        # menos RPCs a HBase a cambio de <= pausa_lote_chico de latencia
        self.pausa_lote_chico = pausa_lote_chico
        self.r = conectar_redis()
        self.lotes = 0
        self.procesadas = 0
        self.rescatadas = 0
        self.revisar_propios = True
        self.ultimo_rescate = 0.0

    # ---------------------------------------------------------------------------------------
    def log(self, msg):
        print(f"[{self.grupo}/{self.nombre}] {msg}", flush=True)

    def asegurar_grupo(self):
        try:
            # id 0: el grupo ve el stream completo desde el principio (si se recrea tras perder Redis, igual)
            self.r.xgroup_create(self.stream, self.grupo, id="0", mkstream=True)
            self.log("grupo creado")
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def manejar(self, entradas):
        ids = [i for i, _ in entradas]
        eventos = [c for _, c in entradas if c]   # una entrada recortada/borrada llega sin campos
        if not ids:
            return
        self.lotes += 1
        if self.ack_antes:   # MAL a proposito (D.1, variante): ack primero, put despues
            self.r.xack(self.stream, self.grupo, *ids)
            if self.morir_en_lote == self.lotes:
                self.log(f"MUERO despues del XACK y antes del put (lote {self.lotes}, {len(ids)} entradas)")
                os._exit(1)
            self.procesar(eventos)
        else:
            self.procesar(eventos)
            if self.morir_en_lote == self.lotes:
                self.log(f"MUERO despues del put y antes del XACK (lote {self.lotes}, {len(ids)} entradas)")
                os._exit(1)
            self.r.xack(self.stream, self.grupo, *ids)
        self.procesadas += len(ids)
        self.r.incrby(f"stats:{self.stream}:{self.grupo}:procesadas", len(ids))

    def pendientes_propios(self):
        """XREADGROUP con id 0: lo que este mismo consumidor leyo y no alcanzo a confirmar."""
        while True:
            resp = self.r.xreadgroup(self.grupo, self.nombre, {self.stream: "0"}, count=self.lote)
            entradas = resp[0][1] if resp else []
            if not entradas:
                break
            self.log(f"reprocesando {len(entradas)} pendientes propios")
            self.manejar(entradas)
        self.revisar_propios = False

    def rescatar(self):
        """XAUTOCLAIM: se queda con lo pendiente de consumidores que llevan > min_idle_ms sin confirmar (muertos)."""
        inicio = "0-0"
        while True:
            resp = self.r.xautoclaim(self.stream, self.grupo, self.nombre, self.min_idle_ms,
                                     start_id=inicio, count=self.lote)
            inicio, entradas = resp[0], resp[1]
            borradas = resp[2] if len(resp) > 2 else []
            if borradas:
                self.log(f"XAUTOCLAIM: {len(borradas)} pendientes ya no estaban en el stream (recortadas): se pierden")
            if entradas:
                self.log(f"XAUTOCLAIM: rescate {len(entradas)} entradas de otro consumidor")
                self.rescatadas += len(entradas)
                self.manejar(entradas)
            if inicio in ("0-0", b"0-0"):
                break
        self.ultimo_rescate = time.time()

    def terminado(self):
        if not self.r.exists(f"{self.stream}:fin"):
            return False
        for g in self.r.xinfo_groups(self.stream):
            if g["name"] == self.grupo:
                if g["pending"]:
                    return False
                if g.get("lag") is not None:
                    return g["lag"] == 0
                return g["last-delivered-id"] == self.r.xinfo_stream(self.stream)["last-generated-id"]
        return False

    # ---------------------------------------------------------------------------------------
    def correr(self):
        self.log(f"inicio (stream {self.stream}, lote {self.lote})")
        while True:
            try:
                if self.revisar_propios:
                    self.asegurar_grupo()
                    self.pendientes_propios()
                if time.time() - self.ultimo_rescate > 2:
                    self.rescatar()
                resp = self.r.xreadgroup(self.grupo, self.nombre, {self.stream: ">"}, count=self.lote, block=500)
                if resp and resp[0][1]:
                    self.manejar(resp[0][1])
                    if len(resp[0][1]) < self.lote and self.pausa_lote_chico:
                        time.sleep(self.pausa_lote_chico)
                elif self.salir_al_terminar and self.terminado():
                    break
            except (redis.ConnectionError, redis.TimeoutError, redis.BusyLoadingError) as e:
                self.log(f"Redis no disponible ({type(e).__name__}), reintento")
                self.revisar_propios = True
                time.sleep(0.5)
            except redis.ResponseError as e:
                if "NOGROUP" in str(e) or "no such key" in str(e):
                    self.log("el grupo no existe (Redis perdio los datos?), lo recreo desde 0")
                    self.revisar_propios = True
                    continue
                raise
            except Exception as e:  # noqa: BLE001  (p.ej. HBase caido): no hay XACK, se reintenta
                self.log(f"error procesando: {type(e).__name__}: {e} -> reintento sin XACK")
                self.revisar_propios = True
                time.sleep(1)
        self.log(f"fin: {self.procesadas:,} entradas procesadas en {self.lotes} lotes "
                 f"({self.rescatadas} rescatadas de otros)")
