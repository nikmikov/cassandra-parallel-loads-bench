package com.mikoff.pg

import scala.util.Try

object FPUtil {
  implicit class PimpTry[A](t: Try[A]) {
    /** Defer the cleanup of a resource until after evaluation, regardless of success. */
    def defer[B](deferred: A => AnyVal)(body: A => Try[B]): Try[B] = {
      val result: Try[B] = t.flatMap(body)
      t.flatMap(a => Try(deferred(a))).flatMap(_ => result)
    }
  }
}
