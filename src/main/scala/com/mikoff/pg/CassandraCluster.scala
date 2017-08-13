package com.mikoff.pg

import com.datastax.driver.core._
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import scala.util.Try

case class CassandraCluster(host: String, port: Int, parallel: Int) {

  import FPUtil._

  private lazy val cluster = Cluster.builder()
    .addContactPoint(host)
    .withPort(port)
    .withPoolingOptions(poolingOptions)
    .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder.build))
    .build()

  def execute[T](f: Session => T): Try[T] = Try(cluster.connect).defer(_.close)(s=>Try(f(s)))

  def close():Unit = if (!cluster.isClosed) cluster.close

  private def poolingOptions: PoolingOptions = {
    new PoolingOptions()
      .setMaxConnectionsPerHost(HostDistance.LOCAL,  parallel)
  }

}
