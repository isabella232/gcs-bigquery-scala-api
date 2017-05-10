package com.emarsys.google.bigquery.builder

import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.duration.FiniteDuration


sealed trait QueryCondition {
  def show: String
  protected def alias(table: Option[Table]) = table.map(_.alias + ".").getOrElse("")
}

object QueryCondition {

  case class BetweenCondition(fieldName: String, startDate: DateTime, endDate: DateTime, table: Option[Table] = None) extends QueryCondition {
    val fromSqlDatetime = startDate.withZone(DateTimeZone.UTC).toString("YYYY-MM-dd HH:mm:ss")
    val toSqlDatetime = endDate.withZone(DateTimeZone.UTC).toString("YYYY-MM-dd HH:mm:ss")

    override def show = s"""${alias(table)}$fieldName BETWEEN TIMESTAMP("$fromSqlDatetime UTC") AND TIMESTAMP("$toSqlDatetime UTC")"""
  }

  case class StringEqualsCondition(fieldName: String, value: String, table: Option[Table] = None) extends QueryCondition {
    override def show = s"${alias(table)}$fieldName = $value"
  }

  case class LessOrEqualsCondition(fieldName: String, value: Int, table: Option[Table] = None) extends QueryCondition {
    override def show = s"${alias(table)}$fieldName <= $value"
  }

  case class GreaterOrEqualsCondition(fieldName: String, value: Int, table: Option[Table] = None) extends QueryCondition {
    override def show = s"${alias(table)}$fieldName >= $value"
  }

  case class IsInTheLastDurationCondition(fieldName: String, duration: FiniteDuration, table: Option[Table] = None) extends QueryCondition {
    override def show = alias(table) + fieldName + s""" > DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -${duration.toSeconds}, "SECOND")"""
  }

  case object EmptyCondition extends QueryCondition {
    override def show = ""
  }

  case class IsNullCondition(fieldName: String, table: Option[Table] = None) extends QueryCondition {
    override def show = alias(table) + fieldName + " IS NULL"
  }

  case class Disjunction(cs: Seq[QueryCondition]) extends QueryCondition {
    override def show =
      cs.map(_.show).filter(_.nonEmpty).foldLeft(QueryCondition.empty.show)((c1, c2) => if (c1.nonEmpty) s"($c1 OR $c2)" else c2)
  }

  case class Conjunction(cs: Seq[QueryCondition]) extends QueryCondition {
    override def show =
      cs.map(_.show).filter(_.nonEmpty).foldLeft(QueryCondition.empty.show)((c1, c2) => if(c1.nonEmpty) s"($c1 AND $c2)" else c2)
  }

  def disjunction(cs: Seq[QueryCondition]): QueryCondition = Disjunction(cs)

  def conjunction(cs: Seq[QueryCondition]): QueryCondition = Conjunction(cs)

  def empty: QueryCondition = EmptyCondition

  implicit class ConditionExtension(condition: QueryCondition) {

    def &&(otherCondition: QueryCondition): QueryCondition = conjunction(Seq(condition, otherCondition))

    def ||(otherCondition: QueryCondition): QueryCondition = disjunction(Seq(condition, otherCondition))

  }

  implicit class FieldExtension(fieldName: String) {

    def ===(value: String): QueryCondition = StringEqualsCondition(fieldName, value)

    def ===(value: Int): QueryCondition = StringEqualsCondition(fieldName, value.toString)

    def <<=(value: Int): QueryCondition = LessOrEqualsCondition(fieldName, value)

    def >>=(value: Int): QueryCondition = GreaterOrEqualsCondition(fieldName, value)

    def isInTheLast(duration: FiniteDuration): QueryCondition = IsInTheLastDurationCondition(fieldName, duration)

    def between(from: DateTime, to: DateTime): QueryCondition = BetweenCondition(fieldName, from, to)

  }

}
