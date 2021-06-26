import threading
from numbers import Number
from typing import Dict

from py4j.java_collections import JavaIterator
from py4j.java_gateway import JavaObject
from pyspark.sql import SparkSession


# noinspection PyPep8Naming
class Listener:
    def __init__(self, gateway, barrier: threading.Semaphore):
        self._gateway = gateway
        self._barrier = barrier
        self._metrics: Dict[str, Number] = {}

    def onSuccess(self, _funcname, qe, _durationns):
        m = qe.executedPlan().metrics()
        ks: JavaObject = m.keys().iterator()
        ks: JavaIterator = self._gateway.jvm.scala.collection.JavaConverters.asJavaIterator(ks)
        for k in ks:
            self._metrics[k] = m.get(k).value().value()
        self._barrier.release()

    def onFailure(self, _funcname, _qe, _durationns):
        self._barrier.release()

    class Java:
        implements = ["org.apache.spark.sql.util.QueryExecutionListener"]


# noinspection PyProtectedMember
class SparkMetrics:
    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._semaphore = threading.Semaphore(0)
        self._listener = Listener(spark.sparkContext._gateway, self._semaphore)

    def __enter__(self) -> Dict[str, Number]:
        gateway = self._spark.sparkContext._gateway
        gateway.callback_server_parameters.daemonize = True
        gateway.callback_server_parameters.daemonize_connections = True
        gateway.start_callback_server()
        self._spark._jsparkSession.listenerManager().register(self._listener)
        return self._listener._metrics

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._semaphore.acquire()
        self._spark._jsparkSession.listenerManager().unregister(self._listener)
