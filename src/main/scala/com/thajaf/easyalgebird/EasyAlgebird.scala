package com.thajaf.easyalgebird

import com.twitter.scalding.TypedPipe

import com.twitter.algebird.Aggregator._

import com.twitter.algebird.{ MonoidAggregator, Aggregator }

import java.nio.ByteBuffer

/** A maeasure represents an aggregated value */
sealed trait Measure

case class AggregatedData[P, S, V]( primaryKey: P, list: List[AttributeValue[S, V]] )

case class PivotedData[T <: Product, V]( data: T, pivotedField: V )

case class AttributeValue[S, V]( attributeName: S, value: V )

/** A bunch of handy utilities to keep off subtle complications of aggregation, pivoting through out the project */
object EasyAlgebird {
  /** Aggregate data set in scalding, given an input pipe,
    * and primary key and aggregator, which can be a join
    * of all rules
    */
  def getAggregatedOutput[T <: ThriftStruct, P: Ordering, B, S, V](
    data:         TypedPipe[T],
    primaryKey:   T => P,
    agg:          Aggregator[T, B, List[AttributeValue[S, V]]],
    reducerCount: Int                                          = 108
  ): TypedPipe[AggregatedData[P, S, V]] =
    data.groupBy( primaryKey )
      .aggregate( agg )
      .withReducers( reducerCount )
      .toTypedPipe
      .map { case ( key, av ) => AggregatedData( key, av ) }

  /** A handy method to convert a map
    * that can exist in a data to List of `AttributeValue`
    * where the attribute name and its value are clearly specified.
    * Pivoting utilities are based on this.
    */
  private def toListOfApv[S, V]( data: Map[S, V] ): List[AttributeValue[S, V]] =
    data.toList.map { case ( a, v ) => AttributeValue( a, v ) }

  /** Method to transform the primary key in the
    * aggregated data. This can be a requirement
    */
  def transformAggregatedData[T <: ThriftStruct, P: Ordering, S, V](
    aggregatedData: TypedPipe[AggregatedData[P, S, V]],
    f:              P => T
  ): TypedPipe[T] = aggregatedData.map( agg => f( agg.primaryKey ) )

  /** Method to do chained feature generation.
    * This is  to do a real
    * transformation based on aggregated data and emit all of them
    * as one data set.
    */
  private def addMoreFeaturesToAggData[T <: ThriftStruct, P: Ordering, S, V](
    aggregatedData: TypedPipe[AggregatedData[P, S, V]],
    f:              AggregatedData[P, S, V] => List[AttributeValue[S, V]]
  ) = aggregatedData.map { agg => AggregatedData( agg.primaryKey, agg.list ++ f( agg ) ) }

  /** Method to convert an aggregated data to
    * pivoted data
    */
  def pivotAggregatedData[T <: Product, P, S, V](
    typedPipe:         TypedPipe[AggregatedData[P, S, V]],
    f:                 ( P, AttributeValue[S, V] ) => T,
    pivotBasedOnField: AggregatedData[P, S, V] => List[AttributeValue[S, V]] = ( agg: AggregatedData[P, S, V] ) => agg.list
  ): TypedPipe[T] = {
    def convertAggregationToOutput( x: AggregatedData[P, S, V] ): List[T] = {
      pivotBasedOnField( x ).map { eachParValue => f( x.primaryKey, eachParValue ) }
    }

    typedPipe.flatMap( convertAggregationToOutput )
  }

  /** Pivot Simple Data on Map, further abstraction was possible, but didn't do */
  def pivotDataOnSimpleMap[T <: Product, U <: Product, S, V](
    typedPipe:         TypedPipe[T],
    f:                 ( T, AttributeValue[S, V] ) => U,
    pivotBasedOnField: T => Map[S, V]
  ): TypedPipe[U] =
    pivotAggregatedData( typedPipe.map( t => AggregatedData( t, toListOfApv( pivotBasedOnField( t ) ) ) ), f )

  /** to pivot based a data based on a field, that can potentially be a list, further abstraction was possible though */
  def pivotDataOnArray[T1 <: Product, T2 <: Product, V](
    typedPipe:       TypedPipe[T1],
    pivotBasedField: T1 => List[V],
    f:               PivotedData[T1, V] => T2
  ): TypedPipe[T2] = typedPipe.flatMap( t => pivotBasedField( t ).map( eachValue => f( PivotedData( t, eachValue ) ) ) )

  /** find max value given an aggregated data set
    */
  def findMaxInAggregation[T <: ThriftStruct, M: Ordering](
    maxOfField: T => M
  ): Aggregator[T, T, M] = maxBy[T, M]( maxOfField ).andThenPresent( maxOfField )

  /** find min value given an aggregated data set
    */
  def findMinInAggregation[T <: ThriftStruct, M: Ordering](
    minOfField: T => M
  ): Aggregator[T, T, M] = minBy[T, M]( minOfField ).andThenPresent( minOfField )

  /** Count the number of rows, given
    * an aggregated data set, based on a condition
    */
  def countValue[T <: ThriftStruct](
    condition:   T => Boolean,
    measureName: Measure
  ) = com.twitter.algebird.Aggregator.count( condition ).andThenPresent( t => AttributeValue( measureName.toString, t ) )

  /** sum of a field in an aggregated
    * data set
    */
  def sumValue[T <: ThriftStruct](
    field:       T => Long,
    measureName: Measure
  ): MonoidAggregator[T, Long, AttributeValue[String, Long]] = prepareMonoid( field ).andThenPresent( t => AttributeValue( measureName.toString, t ) )

  /** `Ordering` implementation for a `ThriftStruct`
    * This is an already implemented version in util, except
    * we get rid of thriftstruct dependency here.
    */
  class ThriftOrder[T <: Product] extends Ordering[T] {
    def compare( x: T, y: T ): Int =
      ( 0 to x.productArity - 1 )
        .find( i => compareItems( x.productElement( i ), y.productElement( i ) ) != 0)
        .getOrElse( 0 )

    def compareItems( left: Any, right: Any ): Int = {
      ( left, right ) match {
        case ( _: Unit, _: Unit )             => 0
        case ( None, None )                   => 0
        case ( Some( _ ), None )              => -1
        case ( None, Some( _ ) )              => 1
        case ( Some( x ), Some( y ) )         => compareItems( x, y )
        case ( x: Boolean, y: Boolean )       => x.compareTo( y )
        case ( x: Double, y: Double )         => x.compareTo( y )
        case ( x: Short, y: Short )           => x.compareTo( y )
        case ( x: Byte, y: Byte )             => x.compareTo( y )
        case ( x: Long, y: Long )             => x.compareTo( y )
        case ( x: String, y: String )         => x.compareTo( y )
        case ( x: ByteBuffer, y: ByteBuffer ) => x.compareTo( y )
        case ( x: Int, y: Int )               => x.compareTo( y )
        case ( x: Map[_, _], y: Map[_, _] )   => if ( x == y ) 0 else -1
        case ( x: Seq[_], y: Seq[_] )         => if ( x == y ) 0 else -1
        case _ =>
          throw new UnsupportedOperationException(
            s"found types that cannot be compared (for $left and $right ) "
          )
      }
    }
  }
}

/** Algebird aggregations always
  * play with nested tuple, and shapeless
  * will be a good tool to flatten
  * the nested tuple
  */
object Flatten {
  import shapeless._
  import shapeless.ops.tuple.FlatMapper
  import shapeless.syntax.std.tuple._

  trait LowPriorityFlatten extends Poly1 {
    implicit def default[T] = at[T]( Tuple1( _ ) )
  }

  object flatten extends LowPriorityFlatten {
    implicit def caseTuple[P <: Product](
      implicit
      fm: FlatMapper[P, flatten.type]
    ) =
      at[P]( _.flatMap( flatten ) )
  }
}
