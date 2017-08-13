package com.mikoff.pg

case class Timer(intervalSec: Int, f:() => Unit) {

  val timer = new java.util.Timer()

  val task = new java.util.TimerTask {
    def run() = f()
  }

  timer.schedule(task, intervalSec * 1000, intervalSec * 1000)

  def close():Unit = timer.cancel()

}
