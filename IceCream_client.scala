import scala.collection.IterableOnce.iterableOnceExtensionMethods

/*
    Public Interface for all IceCream functionality
      * after hidden implementation IceCream object is
        created, client has this interface to invoke methods
        and do all transformations
 */
trait IceCream{
  import IceCream._  // make type definitions in companion object visible here

  // the IceCream hidden implementation should have the only copy
  // of the source data.  this allows client to do additional
  // transformations as needed with client-defined methods
  def getRows: RowSeq

  /* individual operations */
  def getFlavorSet(flavors: Set[String]) : RowSet
  def getMakerLikeSet(likeString: String): RowSet
  // these use symbol aliases for Set union and intersection
  def unionSets    (left: RowSet, right: RowSet): RowSet = left | right
  def intersectSets(left: RowSet, right: RowSet): RowSet = left & right

  // equivalent to T-SQL 'query rewrite II'
  def queryResult(intersectFlavorSet: Set[String])
                 (unionMakerLike: String,
                  unionFlavorSet: Set[String]): RowSet
  // same as above using function composition
  def queryResult_compose(intersectFlavorSet: Set[String])
                           (unionMakerLike: String,
                            unionFlavorSet: Set[String]): RowSet

  // optimized out as per T-SQL analysis but still callable - testing only
  @deprecated def getBaseFlavorSet(baseFlavors: Set[String]): RowSet
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
    (rows: RowSeq) holds all source data
   */
  private class IceCreamImpl(rows: RowSeq) extends IceCream{
    /*
        * each Map structure simulates a T-SQL index
        * trait IndexMap simply groups case classes
     */
    trait IndexMap

    // mimics the primary key index PK_MakerFlavor
    case class MF(maker: String, flavor: String) extends IndexMap
    val makerFlavor_baseF_map: Map[MF, String] =
      // 1st param list: set key to MF; 2nd: specify base flavor value
      rows.groupMap[MF,String]((row: Row) => MF(row._1,row._2))(_._3)
        // convert value: Seq[String] to just String since (Maker,Flavor) -> BaseFlavor
        .flatMap((kv: (MF,Seq[String])) =>
          kv._2 map( baseF => kv._1 -> baseF))

    // build the IX_MakerFlavor_Flavor covering index from the primary key above
    case class BM(BaseFlavor: String, maker: String) extends IndexMap
    val flavor_bm_map: Map[String, Iterable[BM]] =
      makerFlavor_baseF_map
      .groupMap((x: (MF,String)) => x._1.flavor)(x => BM(x._2,x._1.maker))

    // used only by deprecated function getBaseFlavorSet()
    // FOR TESTING ONLY either remove or reinstate function
    val baseFlavor_mf_map: Map[String, Seq[MF]] =
                            rows.groupMap(_._3)(x => MF(x._1, x._2))
    /*--------------*/

    // invoked by getMakerLikeSet() below to filter out 'non-Creamery' makers
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
      NOTE: may cause incorrect return if any flavors not recognized or missing -
            ie no exception handling for unrecognized flavors
    */
    def getFlavorSet(flavors: Set[String]): RowSet = {
      // for each flavor to find...
      // first pass through only flavors in the flavor map - unrecognized flavors ignored
      flavors.withFilter(flavor => flavor_bm_map.contains(flavor)).
              flatMap[Row]((flavor: String) =>
                // find it IMMEDIATELY by key in the Map structure - guaranteed there
                flavor_bm_map(flavor).
                  map[Row]((bm: BM) => (bm.maker, flavor, bm.BaseFlavor)))
    }

    // inefficient in T-SQL and worse here: must test every maker-flavor combination
    // can optimize somewhat - see getMakerLikeSet_optimal next
    def getMakerLikeSet(likeString: String): RowSet = {
      // non-strict filter using 'def makerLikeMatch'
      makerFlavor_baseF_map.keySet.withFilter(
        (mf: MF) => makerLikeMatch(mf.maker, likeString))
          // revisit primary key to get base flavor
          .map(mf => (mf.maker, mf.flavor, makerFlavor_baseF_map(mf)))
    }

    // efficient version of above - use this for production
    // requires rewrite of queryResult() versions though...
    def getMakerLikeSet_optimal(likeString: String, keys: Set[MF]): RowSet =
      keys.withFilter(mf => makerLikeMatch(mf.maker, likeString))
        .map[Row](mf => (mf.maker, mf.flavor, makerFlavor_baseF_map(mf)))

    // may be useful in future for other manipulations - can reverse deprecation
    @deprecated("inefficient for the data transformation as per T-SQL analysis - use getFlavorSet")
    def getBaseFlavorSet(baseFlavors: Set[String]): RowSet =
      baseFlavors flatMap(bf =>
        baseFlavor_mf_map(bf) map(bf_value =>
                               (bf_value.maker,bf_value.flavor,bf)))
    /*
      solves the requirement: equivalent to T-SQL 'query rewrite II'
    */
    def queryResult(outerFlavors: Set[String])
             (unionMakerLike: String,
              unionFlavorSet: Set[String]): RowSet =

      getFlavorSet(outerFlavors)
        // '&' (intersect) == Nested Loops (Inner Join)
        .&(getMakerLikeSet(unionMakerLike)
            // '|' (union) == (Stream Aggregate <- Concatenation)
            .|(getFlavorSet(unionFlavorSet)))

    /*
      solves the requirement query by function composition
    */
    def queryResult_compose(intersectFlavorSet: Set[String])
                           (unionMakerLike: String,
                            unionFlavorSet: Set[String]): RowSet =
          intersectSets(getFlavorSet(intersectFlavorSet),
                        unionSets(getMakerLikeSet(unionMakerLike),
                                  getFlavorSet(unionFlavorSet)))
  }  // IceCream_impl
}  // IceCream companion

/*-------------------------------------------------------------------------------*/

/*
this simulates the client and testing software
* all side effects done here only!  reading from OS file,
  writing to console...
*/
object IceCream_client extends App{
  import IceCream._    // construct singleton object
  import java.io.{BufferedReader, FileReader}
  import scala.util.{Try, Failure, Success, Using}

  /*
    invoking object IceCream.apply() to create implementation object
    the OS file data source is retrieved and passed in IceCream.apply(<HERE>)
      so only IceCream object has copy of data
   */
  val iceCream: IceCream = {
    IceCream(
    // builds comma-delineated sequence (list) of lines from OS file (path in args(0))
    // the type returned is Try[Seq[String]] ('Using' automatically closes resources)
    Using(new BufferedReader(
      new FileReader(args(0)))){ reader =>
        Iterator.continually(reader.readLine()).takeWhile(_ != null).toSeq
    }
    match {
      // Try has two subclasses, Success and Failure; the "match" allows teasing out Seq[String] for former
      // tail: skip header row; first map(): split each line into words: maker/flavor/base flavor;
      //                        second map(): each row of MFB into three-tuple
      case Success(v) => v.tail.map[Array[String]]((x: String) => x.split(',')).
                            map((x: Array[String]) => (x(0), x(1), x(2)))
      case Failure(e) => println(s"error message: ${e.getMessage}"); throw e
      }
    )
  }
println(s"args(0): ${args(0)}"); println()
  /*-----------------------------------------------*/

  /*
  NOT FOR PRODUCTION: demo/testing only
    * can print each intermediate result set
  */
  // the only copy should be in the library as set above except as noted for experimental querying
  //

  println("/*-------------------START TESTING-------------------*/"); println()

  val allRows: RowSeq = iceCream.getRows
  println("/*-----------------------ALL ROWS-----------------------*/")
  allRows foreach println; println()

  // the three flavors in the T-SQL outer input to Nested Loops operator
  val flavor_set: RowSet = iceCream.getFlavorSet(Set[String]("Mint","Coffee","Vanilla"))
  println("/*-----------------------FLAVORS (INTERSECT OUTER INPUT)-----------------------*/")
  flavor_set foreach println; println()

  // the next twp sets simulate the pair in the UNION part of the T-SQL
  val creamery_set    : RowSet = iceCream.getMakerLikeSet("Creamery")
  println("/*-----------------------CREAMERY MAKERS (UNION)-----------------------*/")
  creamery_set foreach println; println()

  // second use of 'getFlavorSet'
  val flavor_set_union: RowSet = iceCream.getFlavorSet(Set[String]("Vanilla"))
  println("/*-----------------------FLAVOR = Vanilla (UNION)-----------------------*/")
  flavor_set_union foreach println; println()

  // NOT USED as per T-SQL optimization: produces four rows, two of which not used
  // compare this with flavor_set_union (two rows) above
  // the strikeout means DEPRECATED but still callable
  val flavor_set_union_BF: RowSet = iceCream.getBaseFlavorSet(Set("Vanilla"))
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
  this is more 'functional' chaining style and compact - still testing...
  */
  val queryResult_II: RowSet = {
  iceCream.getFlavorSet(Set[String]("Mint","Coffee","Vanilla"))
  .& (iceCream.getMakerLikeSet("Creamery")
    .| (iceCream.getFlavorSet(Set[String]("Vanilla"))))
  }
  println("/*-----------------------COMPACT RESULT-----------------------*/")
  queryResult_II foreach println; println()

  /*------------------END TESTING------------------*/

  println("/*-------------------END TESTING-------------------*/"); println()

  /*
  ACTUAL SOLUTION
    * set values and call queryResult() on iceCream object
  */
  val intersect_flavors = Set[String]("Mint","Coffee","Vanilla")
  val union_flavors  = Set[String]("Vanilla")
  val result: RowSet =
  iceCream.queryResult(intersect_flavors)("Creamery", union_flavors)
  println("/*-----------------------ACTUAL RESULT-----------------------*/")
  result foreach println; println()
  println("/*-----------------------ALTERNATE RESULT-----------------------*/")
  val result_alternate: RowSet = {
  iceCream.queryResult_compose(intersect_flavors)("Creamery", union_flavors)
  }
  result_alternate foreach println
}
