package com.emarsys.google.bigquery.builder


sealed trait QueryCondition {
  def show: String
  protected def alias(table: Option[Table]) = table.map(_.alias + ".").getOrElse("")
}

object QueryCondition {

  case class StringEqualsCondition(fieldName: String, value: String, table: Option[Table] = None) extends QueryCondition {
    override def show = s"${alias(table)}$fieldName = $value"
  }

  case object EmptyCondition extends QueryCondition {
    override def show = ""
  }

  case class IsNullCondition(fieldName: String, table: Option[Table] = None) extends QueryCondition {
    override def show = alias(table) + fieldName + " IS NULL"
  }

  case class Disjunction(cs: Seq[QueryCondition]) extends QueryCondition {
    override def show =
      cs.map(_.show).filter(_.nonEmpty).reduce((c1, c2) => s"($c1 OR $c2)")
  }

  case class Conjunction(cs: Seq[QueryCondition]) extends QueryCondition {
    override def show =
      cs.map(_.show).filter(_.nonEmpty).reduce((c1, c2) => s"($c1 AND $c2)")
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

  }

}
