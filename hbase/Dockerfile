# HBase 2.5 standalone (HMaster + RegionServer + ZooKeeper en un solo proceso) + Thrift en 9090.
# Basado en el Dockerfile del Lab 07. Unico cambio: como se descarga HBase.
# El CDN de Apache (dlcdn) solo deja la ultima version de cada rama (en nov-2026 la 2.5.15 ya da 404
# y esta la 2.5.16). Entonces se prueba: la version fijada en dlcdn -> la ultima 2.5.x que liste dlcdn
# -> archive.apache.org (lento pero siempre esta). Todas son rama 2.5, misma API Thrift.
FROM eclipse-temurin:11-jre

ARG HBASE_VERSION=2.5.16
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && bajar() { echo "Intentando $1"; curl -fSL --retry 3 --progress-bar "$1" -o /tmp/hbase.tgz; } \
 && ULTIMA=$(curl -fsSL https://dlcdn.apache.org/hbase/ | grep -o 'href="2\.5\.[0-9]*/"' | grep -o '2\.5\.[0-9]*' | sort -t. -k3 -n | tail -1 || true) \
 && ( bajar https://dlcdn.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
   || ( [ -n "$ULTIMA" ] && bajar https://dlcdn.apache.org/hbase/${ULTIMA}/hbase-${ULTIMA}-bin.tar.gz ) \
   || bajar https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz ) \
 && tar -xzf /tmp/hbase.tgz -C /opt && mv /opt/hbase-2.5.* /opt/hbase \
 && rm -rf /tmp/hbase.tgz /opt/hbase/docs

ENV HBASE_HOME=/opt/hbase PATH=/opt/hbase/bin:$PATH

# rootdir en disco local (no HDFS), igual que el Lab 07. /data es el volumen con nombre.
RUN printf '%s\n' '<configuration>' \
  '<property><name>hbase.rootdir</name><value>file:///data/hbase</value></property>' \
  '<property><name>hbase.zookeeper.property.dataDir</name><value>/data/zookeeper</value></property>' \
  '<property><name>hbase.unsafe.stream.capability.enforce</name><value>false</value></property>' \
  '<property><name>hbase.hfile.compaction.discharger.interval</name><value>10000</value></property>' \
  '</configuration>' > /opt/hbase/conf/hbase-site.xml

# 16010: web del HMaster (y /jmx con las metricas por region) - 9090: Thrift (happybase)
EXPOSE 16010 9090
WORKDIR /opt/hbase

# SIGTERM (compose down/restart) -> apagado ordenado, HBase vacia la MemStore antes de salir
CMD ["bash", "-c", "trap 'hbase-daemon.sh stop thrift; stop-hbase.sh; exit 0' TERM; start-hbase.sh && hbase-daemon.sh start thrift && tail -F logs/*.log & wait"]
