package com.emarsys.google.bigquery.builder

import cats.Monoid
import org.joda.time.LocalDate

sealed trait QueryCondition {
  def show: String
  protected def alias(table: Option[Table]) = table.map(_.alias + ".").getOrElse("")
}

object QueryCondition {

  case class CustomerCondition(customerId: Int, table: Option[Table] = None) extends QueryCondition {
    override def show =
      s"${alias(table)}customer_id = $customerId"
  }

  case class ContactCondition(contactId: Int, table: Option[Table] = None) extends QueryCondition {
    override def show =
      s"${alias(table)}contact_id = $contactId"
  }

  case class LoadedAtIsInLastDays(days: Int, table: Option[Table] = None) extends QueryCondition {
    override def show =
      s"${alias(table)}loaded_at >= TIMESTAMP(DATE_ADD(CURRENT_DATE(),INTERVAL -$days DAY)) AND ${alias(table)}loaded_at < TIMESTAMP(CURRENT_DATE())"
  }

  case object EmptyCondition extends QueryCondition {
    override def show = ""
  }

  case class LoadedAtIsInDay(date: LocalDate, table: Option[Table] = None) extends QueryCondition {
    override def show = s"DATE(${alias(table)}loaded_at) = '${date.toString}'"
  }

  case class CustomerIdIsNullCondition(table: Option[Table] = None) extends QueryCondition {
    override def show = s"${alias(table)}customer_id IS NULL"
  }

  case class Disjunction(cs: Seq[QueryCondition]) extends QueryCondition {
    override def show =
      cs.map(_.show).filter(_.nonEmpty).reduce((c1, c2) => s"($c1 OR $c2)")
  }

  case class Conjunction(cs: Seq[QueryCondition]) extends QueryCondition {
    override def show =
      cs.map(_.show).filter(_.nonEmpty).reduce((c1, c2) => s"($c1 AND $c2)")
  }

  implicit val conditionMonoid = new Monoid[QueryCondition] {
    def empty                                           = EmptyCondition
    def combine(c1: QueryCondition, c2: QueryCondition) = conjunction(Seq(c1, c2))
  }

  def disjunction(cs: Seq[QueryCondition]): QueryCondition = Disjunction(cs)

  def conjunction(cs: Seq[QueryCondition]): QueryCondition = Conjunction(cs)

  def loadedAtIsYesterday(): QueryCondition =
    LoadedAtIsInLastDays(1)

  def loadedAtIsInLastDays(days: Int): QueryCondition =
    LoadedAtIsInLastDays(days)

  def customer(customerId: Int): QueryCondition =
    CustomerCondition(customerId)

  def contact(contactId: Int): QueryCondition =
    ContactCondition(contactId)

  def empty: QueryCondition =
    EmptyCondition

  def loadedAtIsInDay(date: LocalDate): QueryCondition =
    LoadedAtIsInDay(date)
}
