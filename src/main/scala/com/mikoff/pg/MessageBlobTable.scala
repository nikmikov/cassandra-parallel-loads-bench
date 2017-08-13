package com.mikoff.pg

import com.datastax.driver.core.{Session, PreparedStatement}

object MessageBlobTable extends LoggingSupport {

  val schemaName = "PerformanceTest"
  val tableName = "Messages"

  private val ddl = Seq(
    s"""|CREATE KEYSPACE IF NOT EXISTS ${schemaName}
        | WITH REPLICATION = {
        |    'class' : 'SimpleStrategy',
        |    'replication_factor' : 2
        | }
        |""".stripMargin,
    s"USE $schemaName",
    s"DROP TABLE IF EXISTS $tableName",
    s"""|CREATE TABLE $tableName (
        |  message_id bigint PRIMARY KEY,
        |  content blob
        |)
        |""".stripMargin
  )

  def setup(session: Session): Unit = {
    ddl.foreach(exec(session) _)
  }

  def prepareInsertStatement(session: Session): PreparedStatement = {
    val table = s"${schemaName}.${tableName}"
    val insertStmt = s"INSERT INTO $table(message_id, content) values(?,?)"
    log.debug(s"Preparing statement: $insertStmt")
    session.prepare(insertStmt)
  }

  private def exec(session: Session)(ddlScript: String): Unit = {
    log.debug(s"Running DDL script:\n$ddlScript")
    session.execute(ddlScript)
    log.debug("Script executed successfully")
  }

}
