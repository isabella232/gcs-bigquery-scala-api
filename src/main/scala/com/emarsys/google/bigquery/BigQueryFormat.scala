package com.emarsys.google.bigquery

import com.google.api.services.bigquery.model.{TableCell, TableRow}

import scala.collection.JavaConverters._
import shapeless._
import labelled.{FieldType, field}
import org.joda.time.{DateTime, DateTimeZone}
import com.google.api.client.util.Data._

import scala.util.Try

object api {
  type FormatResult[T] = Either[String, T]

  trait BigQueryFormat[T] {
    def fromTableRow(tr: TableRow): FormatResult[T]
    def toTableRow(t: T): TableRow
  }

  trait BigQueryType[T] {
    def toValue(v: T): AnyRef
    def fromValue(v: AnyRef): T
  }
}

package object format {
  import api._

  implicit object stringPrimitive extends BigQueryType[String] {
    def toValue(s: String) = s
    def fromValue(v: AnyRef) = v.toString
  }

  implicit object intPrimitive extends BigQueryType[Int] {
    def toValue(s: Int) =
      s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) =
      v.toString.toInt
  }

  implicit object DoublePrimitive extends BigQueryType[Double] {
    def toValue(s: Double) =
      s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) =
      v.toString.toDouble
  }


  implicit object FloatPrimitive extends BigQueryType[Float] {
    def toValue(s: Float) =
      s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) =
      v.toString.toFloat
  }

  implicit object BoolPrimitive extends BigQueryType[Boolean] {
    def toValue(s: Boolean) =
      s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) =
      v.toString.toBoolean
  }

  implicit object DatePrimitive extends BigQueryType[DateTime] {
    def toValue(s: DateTime) = {
      (s.toDateTime(DateTimeZone.UTC).getMillis / 1000).toString.asInstanceOf[AnyRef]
    }

    def fromValue(v: AnyRef) = {
      val d = v.toString.toDouble
      val l = (d * 1000).toLong
      new org.joda.time.DateTime(l, DateTimeZone.UTC)
    }
  }

  implicit def optionType[T](implicit  tType : BigQueryType[T]) : BigQueryType[Option[T]] =
    new BigQueryType[Option[T]] {
      override def toValue(v: Option[T]): AnyRef = (v map tType.toValue).orNull

      override def fromValue(v: AnyRef): Option[T] = if (isNull(v)) None else {
        Try(tType.fromValue(v)).toOption
      }
    }

  implicit def optionFormat[T](implicit tFormat : BigQueryFormat[T]): BigQueryFormat[Option[T]] =
    new BigQueryFormat[Option[T]] {
      override def fromTableRow(tr: TableRow): FormatResult[Option[T]] = {
        Right(tFormat.fromTableRow(tr).right.toOption)
      }

      override def toTableRow(t: Option[T]): TableRow = {
        (t map tFormat.toTableRow).get
      }
    }

  implicit object hNilBigQueryFormat extends BigQueryFormat[HNil] {
    def toTableRow(t: HNil) = {
      val tr = new TableRow()
      tr.setF(List[TableCell]().asJava)
    }
    def fromTableRow(m: TableRow) =
      Right(HNil)
  }

  implicit def hListBigQueryFormat[Key <: Symbol, Value, Tail <: HList](
      implicit witness: Witness.Aux[Key],
      valueFormatter: Lazy[BigQueryType[Value]],
      restFormatter: Lazy[BigQueryFormat[Tail]]): BigQueryFormat[FieldType[Key, Value] :: Tail] =
    new BigQueryFormat[FieldType[Key, Value] :: Tail] {
      def toTableRow(t: FieldType[Key, Value] :: Tail): TableRow = {
        val head = valueFormatter.value.toValue(t.head)
        val c = new TableCell
        c.setV(head)
        val rest = restFormatter.value.toTableRow(t.tail)
        val f = rest.getF.asScala.toList
        rest.setF((c :: f).asJava)

      }

      def fromTableRow(m: TableRow): FormatResult[FieldType[Key, Value] :: Tail] = {
        val f: List[TableCell] = m.getF.asScala.toList
        val v: AnyRef = f.head.getV
          val x = valueFormatter.value.fromValue(v)
          val h = field[Key](x)
          m.setF(f.tail.asJava)
          val tail = restFormatter.value.fromTableRow(m)
          tail match {
            case Left(e) => Left(e)
            case Right(t) =>
              Right(h :: t)
          }


      }
    }

  implicit object cNilBigQueryFormat extends BigQueryFormat[CNil] {
    def toTableRow(t: CNil): TableRow =
      new TableRow
    def fromTableRow(m: TableRow) =
      Left("CNil")
  }

  implicit def coproductBigQueryFormat[Key <: Symbol, Value, Rest <: Coproduct](
      implicit witness: Witness.Aux[Key],
      valueFormatter: Lazy[BigQueryFormat[Value]],
      restFormatter: Lazy[BigQueryFormat[Rest]]): BigQueryFormat[FieldType[Key, Value] :+: Rest] =
    new BigQueryFormat[FieldType[Key, Value] :+: Rest] {
      def toTableRow(t: FieldType[Key, Value] :+: Rest) = {
        t match {
          case Inl(h) =>
            val r = valueFormatter.value.toTableRow(h)
            val typeCell = new TableCell
            typeCell.setV(witness.value.name)
            val augmentedFields = typeCell :: r.getF.asScala.toList
            r.setF(augmentedFields.asJava)
          case Inr(r) =>
            restFormatter.value.toTableRow(r)
        }
      }

      def fromTableRow(m: TableRow) = {
        val thisType = m.getF.asScala.headOption.map(_.getV).contains(witness.value.name)
        if (thisType) {
          val rowWithoutType = new TableRow()
          rowWithoutType.setF(m.getF.asScala.tail.toList.asJava)
          valueFormatter.value.fromTableRow(rowWithoutType) match {
            case Right(x) =>
              Right(Inl(field[Key](x)))
            case Left(y) =>
              Left(y)
          }
        } else {
          restFormatter.value.fromTableRow(m) match {
            case Right(x) =>
              Right(Inr(x))
            case Left(y) =>
              Left(y)
          }
        }
      }
    }

  implicit def familyBigQueryFormat[T, Repr](implicit gen: LabelledGeneric.Aux[T, Repr],
                                             reprFormatter: Lazy[BigQueryFormat[Repr]],
                                             tpe: Typeable[T]): BigQueryFormat[T] =
    new BigQueryFormat[T] {
      def toTableRow(t: T) =
        reprFormatter.value.toTableRow(gen.to(t))
      def fromTableRow(m: TableRow) =
        try {
          reprFormatter.value.fromTableRow(m) match {
            case Right(x) =>
              Right(gen.from(x))
            case Left(y) => Left(y)
          }
        } catch {
          case e: Throwable =>
            Left("Format failed: " + e.getMessage)
        }
    }
}

object syntax {
  import api._
  import format._
  implicit class RichBigResult[R](val e: FormatResult[R]) extends AnyVal {
    def getOrThrowError: R = e match {
      case Left(error) =>
        throw new IllegalArgumentException(error)
      case Right(r) => r
    }
  }

  implicit class RichBigDataFormat[T](val t: T) extends AnyVal {
    def toTableRow(implicit s: BigQueryFormat[T]): TableRow = s.toTableRow(t)

  }

  implicit class RichProperties(val row: TableRow) extends AnyVal {
    def as[T](implicit s: BigQueryFormat[T]): T =
      s.fromTableRow(row).getOrThrowError

    def to[T](implicit t: BigQueryType[T]): T =
     t.fromValue(row.getF.get(0).getV)
  }

}
