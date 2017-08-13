package com.mikoff.pg

import java.nio.file._
import java.nio.channels.SeekableByteChannel

case class ArraySlice (
  startId: Long,
  endId: Long,
  blockSize: Int,
  channel: SeekableByteChannel
)

case class InputArray(fileName: String, blockSize: Int, parallel: Int) extends LoggingSupport {

  log.debug(s"Checking input file: $fileName")
  val inputPath = Paths.get(fileName)
  if (!Files.isReadable(inputPath)) {
    throw new IllegalArgumentException(s"File $fileName does not exists or not readable")
  }

  /** Total size in bytes */
  def sizeBytes: Long = Files.size(inputPath)

  /** Number of blocks of size `blockSize` */
  def size: Long = blocksTotal

  log.info(s"$fileName size: ${sizeBytes} bytes")

  private def blocksTotal: Long = scala.math.ceil(sizeBytes.toDouble / blockSize).toLong
  def numSlices = scala.math.max(1, parallel)
  def sliceSize = blocksTotal / numSlices
  log.info(s"Num slices: $numSlices, slice size: $sliceSize")

  private val r = (0 to numSlices - 1).map(_ * sliceSize) :+ blocksTotal
  private val ranges = r.toList.sliding(2, 1).collect { case x::y::Nil => (x, y) }

  /** Slice input array into `parallel` chunks of equal size */
  def slices: List[ArraySlice] = ranges.toList.map {
    case (from, to) => {
      val offsetBegin = from * blockSize
      log.info(s"Slice: startId: $from; $offsetBegin")
      ArraySlice(from, to, blockSize, openChannel(offsetBegin) )
    }
  }

  def close() = {
    slices.foreach(_.channel.close)
  }

  private def openChannel(pos: Long) = {
    val ch = Files.newByteChannel(inputPath, StandardOpenOption.READ)
    ch.position(pos)
    ch
  }

}
