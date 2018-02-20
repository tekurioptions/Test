import findspark
findspark.init("/opt/cloudera/parcels/CDH/lib/spark")
import pyspark
from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
c = SparkContext(master="yarn-client", appName="CashDeposit")
hc = HiveContext(sc)
data = hc.sql("select * from aom_input.aom_delta_new where (co_canal_023sint = 1 AND nb_ecrit1_023sint = 5207) OR (co_canal_023sint = 10 AND nb_ecrit1_023sint = 7007)")
data.write.save('/user/tekurn/CashDepositOozie', format="parquet",mode='overwrite')
sc.stop()