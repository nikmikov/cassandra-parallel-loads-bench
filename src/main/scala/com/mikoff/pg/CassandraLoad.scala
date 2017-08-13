package com.mikoff.pg

import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._

import java.util.concurrent.atomic.AtomicLong
import java.nio.ByteBuffer

import org.rogach.scallop._
import com.datastax.driver.core.{Session, BatchStatement, ResultSetFuture}

object CassandraLoad extends LoggingSupport {

  val className = getClass.getSimpleName
  class CliConf(args: Seq[String]) extends ScallopConf(args) {
    version("CassandraLoadTest (c) 2017 Nikolay Mikov")
    banner(s"Usage: $className [OPTIONS]\n\tOptions:")
    val hostName = opt[String](
      name = "host",
      descr = "Cassandra cluster contact point: hostname",
      default = Some("localhost"), noshort=true)

    val port = opt[Int](
      name = "port",
      descr = "Cassandra cluster contact point: port",
      default = Some(9042), noshort=true)

    val batchSize = opt[Int](
      name = "batch",
      descr = "Unlogged batch size, no batching when equal 1 or less",
      default = Some(1), noshort=true
    )

    val parallel = opt[Int](
      name = "parallel",
      descr = "Number of parallel loads",
      default = Some(1), noshort=true
    )

    val blockSize = opt[Int](
      name = "block-size",
      descr = "Size of message block in bytes",
      default = Some(10), noshort=true
    )

    val asyncMax = opt[Int](
      name = "async-max",
      descr = "Maximum number of active async queries from one worker",
      default = Some(1024), noshort=true
    )

    val inputFile = opt[String](
      name = "input",
      descr = "Input file location",
      required = true, noshort=true
    )

    verify()
  }

  import FPUtil._

  def main(args: Array[String]): Unit = {
    val conf = new CliConf(args)
    main(
      conf.hostName(),
      conf.port(),
      conf.batchSize(),
      conf.parallel(),
      conf.inputFile(),
      conf.blockSize(),
      conf.asyncMax()
    )
  }

  def main(
    hostName: String,
    port: Int,
    batchSize: Int,
    parallel: Int,
    inputFileName: String,
    blockSize: Int,
    asyncMax: Int): Unit =
  {

    log.info(
      s"Starting load: hostname: $hostName, port: $port, " +
        s"batchSize: $batchSize, parallel: $parallel, " +
        s"inputFile: $inputFileName, blockSize: $blockSize"
    )

    Try(CassandraCluster(hostName, port, parallel)).defer(_.close) {
      cluster => Try(InputArray(inputFileName, blockSize, parallel)).defer(_.close) {
        arr => cluster.execute(session => {
          MessageBlobTable.setup(session)
          load(session, arr, parallel, batchSize, asyncMax)
        })
      }.flatten
    } match {
      case Success(_) => log.info("Success!")
      case Failure(err) => {
        log.error("Error:", err)
        System.exit(1)
      }
    }
  }

  def printSessionState(session: Session): String = {
    val state = session.getState
    val stateStr = state.getConnectedHosts.asScala.map(
      h => s"$h, conn: ${state.getOpenConnections(h)}, inflight: ${state.getInFlightQueries(h)}"
    )
    s"""Session: ${stateStr.mkString("; ")}"""
  }

  def printFn(
    session: Session,
    counter: AtomicLong,
    totalEntries: Long,
    printInterval: Int): () => Unit =
  {
    var prevVal: Long = 0
    var timeTotal: Long = 0
    val formatter = new java.text.DecimalFormat("0.00")
    () => {
      val curVal = counter.get
      val insertRate = (curVal - prevVal) / printInterval
      timeTotal = timeTotal + printInterval
      val avgRate = curVal / timeTotal
      val percentDone = formatter.format(curVal.toDouble * 100 / totalEntries)
      log.info(
          s"Insert rate [entries/sec]: cur: ${insertRate}, avg: ${avgRate}. " +
          s"Total inserted: ${curVal} (${percentDone}%). " +
          printSessionState(session)
      )
      prevVal = curVal
    }
  }

  def load(
    session: Session,
    inputArray: InputArray,
    parallel: Int,
    batchSize: Int,
    asyncMax: Int): Try[Long] =
  {
    import ExecutionContext.Implicits.global

    val counter = new AtomicLong()
    val printInterval: Int = 2 // seconds
    val printer = printFn(session, counter, inputArray.size, printInterval)
    val futures = inputArray.slices.map(createWorker(session, counter, _, batchSize, asyncMax))

    val res = Try(Timer(printInterval, printer)).defer(_.close) { _ => {
      val futureResult = Future.reduceLeft(futures)(_+_)
      Try(Await.result(futureResult, Duration.Inf))
    }}

    log.info(s"Result: $res")
    res
  }

  def createWorker(
    session: Session,
    counter: AtomicLong,
    slice: ArraySlice,
    batchSize: Int,
    asyncMax: Int)
    (implicit ec: ExecutionContext): Future[Long] =
  {

    val stmt = MessageBlobTable.prepareInsertStatement(session)
    val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)

    val byteBuffer = ByteBuffer.allocate(slice.blockSize)
    val lastIx = slice.endId - 1
    def insertOp(ix: Long): Option[ResultSetFuture] = {
      byteBuffer.clear()
      val bytesRead = slice.channel.read(byteBuffer)
      if (bytesRead > 0) {
        val bound = stmt.bind()
        bound.setLong(0, ix)
        bound.setBytes(1, byteBuffer)
        counter.incrementAndGet
        if (batchSize <= 1) {
          Some(session.executeAsync(bound))
        } else {
          batch.add(bound)
          if (batch.size == batchSize || ix == lastIx) {
            val f = session.executeAsync(batch)
            batch.clear()
            Some(f)
          } else None
        }
      } else None
    }

    Future {
      // batch executeAsync queries into `asyncMax` batches,
      // so we don't flood driver's connecton pool
      Stream.range(slice.startId, slice.endId)
        .grouped(asyncMax)
        .map(_.flatMap(insertOp(_)).toList.map(_.getUninterruptibly))
        .flatten
        .length
    }
  }
}
