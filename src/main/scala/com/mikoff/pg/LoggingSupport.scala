package com.mikoff.pg

import org.slf4j.LoggerFactory

trait LoggingSupport {
  protected lazy val log = LoggerFactory.getLogger(getClass)
}
