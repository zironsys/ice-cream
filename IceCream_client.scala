
/*
    Public Interface for all IceCream functionality
      * after hidden implementation IceCream object is
        created, client has this interface to invoke methods
 */
trait IceCream{
  import IceCream._  // make type definitions in object visible here

  // the IceCream hidden implementation should have the only copy
  // of the data.  this allows client to have its own copy
  // to do additional transformations rather than/before modifying
  // the library
  def getRows: RowSeq

  // individual operations
  def getFlavorSet(flavors: Seq[String]):         RowSet
  def getMakerLikeSet(likeString: String):        RowSet
  def unionSets    (left: RowSet, right: RowSet): RowSet
  def intersectSets(left: RowSet, right: RowSet): RowSet

  // equivalent to T-SQL 'query rewrite II' using individual operations above
  def queryResult(intersectFlavorSet: Seq[String])
                 (unionMakerLike: String, unionFlavorSet: Seq[String])
                                                : RowSet

  // optimized out as per T-SQL analysis but still callable - testing only
  @deprecated def getBaseFlavorSet(baseFlavors: Seq[String]): RowSet
}

/*
    Companion object that implements the trait
 */
object IceCream{
  type Row = (String,String,String)  // (Maker, Flavor, BaseFlavor)
  type RowSeq = Seq[Row]             // Seq is base trait for ordered sequence collections such as list...
  type RowSet = Set[Row]

  // factory method the client invokes to create the hidden
  // implementation object and get back 'trait IceCream' of available
  // operations
  def apply(rows: RowSeq): IceCream = {new IceCreamImpl(rows)}

  /*--------------------------------------------------------------*/

  /*
    this is the hidden implementation class as created by
    'def apply()' above.  all data structures are initialized
    as part of primary constructor
    (rows: RowSeq) holds all data gotten from client in 'def apply()'
   */
  private class IceCreamImpl(rows: RowSeq) extends IceCream{

    /*
        each Map structure simulates a T-SQL index

        trait MapValue groups tuple-two case classes

        each of three Maps has a key taken from one
        component of Row and the value in a case class of the other
        two Row components
     */
    trait MapValue
    case class BF(baseFlavor: String, flavor: String) extends MapValue
    val maker_bf_map: Map[String, Seq[BF]] =
      rows.groupMap(_._1)((x: Row) => BF(x._3, x._2))

    case class BM(BaseFlavor: String, maker: String) extends MapValue
    val flavor_bm_map: Map[String, Seq[BM]] =
      rows.groupMap(_._2)(x => BM(x._3, x._1))

    /*
        only used by function getBaseFlavorSet which is deprecated
        can possibly comment out or remove
     */
    case class MF(maker: String, flavor: String) extends MapValue
    val baseFlavor_mf_map: Map[String, Seq[MF]] =
      rows.groupMap(_._3)(x => MF(x._1, x._2))

    /*---------*/

    // invoked by "def getMakerLikeSet" below to filter out 'non-Creamery' makers
    // matches on last word in maker name
    def makerLikeMatch(maker: String, matchSuffix: String): Boolean = {
      // split maker name into array of words and test last word
      maker.split(' ') match{
        case ar: Array[_] if ar(ar.length - 1) == matchSuffix => true
        case _                                                => false
      }
    }

    /*--------------------INTERFACE--------------------*/

    // implements the trait methods

    // all the data if needed by client for custom transformations
    def getRows: RowSeq = rows

    /*
        efficient algorithm analogous to T-SQL index seek on IX_MakerFlavor_Flavor
     */
    def getFlavorSet(flavors: Seq[String]): RowSet = {
      // for each flavor to find...
      flavors.flatMap( flavor =>
        // find it IMMEDIATELY by key in the Map structure and create Row
        // 'bm' is BM case class defined above
        flavor_bm_map(flavor) map(bm => (bm.maker, flavor, bm.BaseFlavor))
      ).toSet
    }

    /*
        not efficient in isolation in the T-SQL and likewise here
        because '%Creamery' is not a SARG, T-SQL does index scan over
        covering index IX_MakerFlavor_Flavor.  this is scan over all
        rows here.  (optimize by adding 'Creamery' key to maker_bf_map
        but since 'Creamery' is not a maker, that would be an error-prone hack)
     */
    def getMakerLikeSet(likeString: String): RowSet = {
      maker_bf_map.keySet withFilter(key =>
        makerLikeMatch(key, likeString)) flatMap( key =>
          maker_bf_map(key) map(bf => (key,bf.flavor,bf.baseFlavor)))
    }

    // may be useful in future for other manipulations - can reverse deprecation
    @deprecated("inefficient for the data transformation as per T-SQL analysis - use getFlavorSet")
    def getBaseFlavorSet(baseFlavors: Seq[String]): RowSet = {
      (baseFlavors flatMap(bf =>
        baseFlavor_mf_map(bf) map(bf_value => (bf_value.maker,bf_value.flavor,bf)))).toSet
    }

    // uses symbol aliases for Set union and intersection
    def unionSets    (left: RowSet, right: RowSet): RowSet = left | right
    def intersectSets(left: RowSet, right: RowSet): RowSet = left & right

    /*
        solves the requirement query
     */
    def queryResult(intersectFlavorSet: Seq[String])
                   (unionMakerLike: String, unionFlavorSet: Seq[String])
                                                              : RowSet = {
      // either of these works -- comment out one or the other
            // chaining functions style
            getFlavorSet(intersectFlavorSet)
            .& (getMakerLikeSet(unionMakerLike)
                  .| (getFlavorSet(unionFlavorSet)))
            // nested functions
            /*
            intersectSets(getFlavorSet(intersectFlavorSet),
                          unionSets(getMakerLikeSet(unionMakerLike),
                                    getFlavorSet(unionFlavorSet)))
            */
    }
  }
}

/*-------------------------------------------------------------------------------*/

/*
    this simulates the client and testing software
      * all side effects done here only!  reading from OS file,
        writing to console...
 */
object IceCream_client extends App{
  import IceCream._
  import java.io.{BufferedReader, FileReader}
  import scala.util.{Failure, Success, Using}

  val iceCream: IceCream = {
    // invoking object IceCream.apply() to create implementation object
    // the OS file data source is retrieved and passed in IceCream.apply(<HERE>)
    // so only library has copy of data
    // NOTE: typing out 'IceCream.apply()' not necessary: IceCream(...) auto invokes
    //       the apply() factory method
    IceCream(
      // builds comma-delineated sequence (list) of lines from file
      // the type returned is Try[Seq[String]] ('Using' automatically closes resources)
      Using(new BufferedReader(
        new FileReader(args(0)))){ reader =>
          Iterator.continually(reader.readLine()).takeWhile(_ != null).toSeq
      }
      match {
        // Try has two subclasses, Success and Failure; the "match" allows teasing out Seq[String] for former
        // tail: skip header row; first map(): split each line into words;
        //                        second map(): each row of words into three-tuple
        case Success(v) => v.tail.map((x: String) => x.split(',')).
                              map((x: Array[String]) => (x(0), x(1), x(2)))
        case Failure(e) => println(s"error message: ${e.getMessage}"); throw e
      }
    )
  }

  // the only copy should be in the library as set above except as noted for experimental querying
  // demo/testing only
  val allRows: RowSeq = iceCream.getRows
  println("/*-----------------------ALL ROWS-----------------------*/")
  allRows foreach println; println()

  // the three flavors in the T-SQL outer input to Nested Loops operator
  val flavor_set: RowSet = iceCream.getFlavorSet(Seq[String]("Mint","Coffee","Vanilla"))
  println("/*-----------------------FLAVORS (INTERSECT OUTER INPUT)-----------------------*/")
  flavor_set foreach println; println()

  // the next twp sets simulate the pair in the UNION part of the T-SQL
  val creamery_set    : RowSet = iceCream.getMakerLikeSet("Creamery")
  println("/*-----------------------CREAMERY MAKERS (UNION)-----------------------*/")
  creamery_set foreach println; println()

  // second use of 'getFlavorSet'
  val flavor_set_union: RowSet = iceCream.getFlavorSet(Seq[String]("Vanilla"))
  println("/*-----------------------FLAVOR = Vanilla (UNION)-----------------------*/")
  flavor_set_union foreach println; println()

  // NOT USED as per T-SQL optimization: produces four rows, two of which not used
  // compare this with flavor_set_union (two rows) above
  // the strikeout means DEPRECATED but still callable
  val flavor_set_union_BF: RowSet = iceCream.getBaseFlavorSet(Seq("Vanilla"))
  println("/*-----------------------BASE FLAVOR = Vanilla (DEPRECATED)-----------------------*/")
  flavor_set_union_BF foreach println; println()

  // take the two sets above and create their union
  // (the strict superset of flavor_set above)
  val union_set: RowSet = iceCream.unionSets(creamery_set, flavor_set_union)
  println("/*-----------------------CREAMERY UNION FLAVOR (INTERSECT INNER INPUT)-----------------------*/")
  union_set foreach println; println()

  // apply the equivalent of the T-SQL INTERSECT operator
  val intersection_set: RowSet = iceCream.intersectSets(flavor_set, union_set)
  println("/*-----------------------QUERY RESULT-----------------------*/")
  intersection_set foreach println; println()

  /*
    SOLUTION II
      the first solution may be preferred for its clarity/readability
      this one is more 'functional' chaining style and compact - what I would
      normally write after developing and testing above to break the problem down
 */
  val queryResult_II: RowSet = {
    iceCream.getFlavorSet(Seq[String]("Mint","Coffee","Vanilla"))
      .& (iceCream.getMakerLikeSet("Creamery")
        .| (iceCream.getFlavorSet(Seq[String]("Vanilla"))))
  }
  println("/*-----------------------COMPACT RESULT-----------------------*/")
  queryResult_II foreach println; println()

  /*
      LIBRARY SOLUTION
        still, the main query to satisfy belongs in the trait and implementation class
        this is my preferred final form
   */

  val intersect_flavors = Seq[String]("Mint","Coffee","Vanilla")
  val union_flavors  = Seq[String]("Vanilla")
  val result: RowSet =
      iceCream.queryResult(intersect_flavors)("Creamery", union_flavors)
  println("/*-----------------------LIBRARY RESULT-----------------------*/")
  result foreach println
}
