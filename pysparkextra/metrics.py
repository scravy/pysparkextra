import logging
import threading
from numbers import Number
from typing import Dict, Optional

from py4j.java_collections import JavaIterator
from py4j.java_gateway import JavaObject
from pyspark.java_gateway import ensure_callback_server_started
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# noinspection PyPep8Naming
class Listener:
    def __init__(self, gateway):
        self._gateway = gateway
        self.semaphore = threading.Semaphore(0)
        self.metrics: Dict[str, Number] = {}

    def onSuccess(self, _funcname, qe, _durationns):
        try:
            m = qe.executedPlan().metrics()
            ks: JavaObject = m.keys().iterator()
            ks: JavaIterator = self._gateway.jvm.scala.collection.JavaConverters.asJavaIterator(ks)
            for k in ks:
                self.metrics[k] = m.get(k).value().value()
        finally:
            self.semaphore.release()

    def onFailure(self, _funcname, _qe, _durationns):
        self.semaphore.release()

    class Java:
        implements = ["org.apache.spark.sql.util.QueryExecutionListener"]


# noinspection PyProtectedMember
class SparkMetrics:
    def __init__(self, spark: SparkSession, timeout: Optional[float] = 15):
        self._spark = spark
        self._listener = Listener(spark.sparkContext._gateway)
        self._timeout = timeout

    def __enter__(self) -> Dict[str, Number]:
        ensure_callback_server_started(self._spark.sparkContext._gateway)
        self._spark._jsparkSession.listenerManager().register(self._listener)
        return self._listener.metrics

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._listener.semaphore.acquire(timeout=self._timeout):
            logger.warning("Did not receive any metrics within %s seconds", self._timeout)
        self._spark._jsparkSession.listenerManager().unregister(self._listener)
