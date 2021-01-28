
import java.io.{BufferedReader, FileReader}
import scala.util.{Failure, Success, Using}

/*
    encapsulates all functionality
 */
object IceCream {

  // captures the data from file
  private val rows: Seq[(String, String, String)] = {
    // using loan pattern, build comma-delineated sequence of lines from file
    // the type returned is Try[Seq[String]]
    Using(new BufferedReader(
          new FileReader(raw"""C:\Users\ziron\IdeaProjects\IceCream_\icecream_rows.csv"""))) { reader =>
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
  }

  /*
      three map structures used to simulate indexes on data
      trait used only for logical grouping
   */
  private trait MapValue
  private case class BF(baseFlavor: String, flavor: String) extends MapValue
  private val maker_bf_map: Map[String, Seq[BF]] =
    rows.groupMap(_._1)((x: Row) => BF(x._3, x._2))

  private case class BM(BaseFlavor: String, maker: String) extends MapValue
  private val flavor_bm_map: Map[String, Seq[BM]] =
    rows.groupMap(_._2)(x => BM(x._3, x._1))

  private case class MF(maker: String, flavor: String) extends MapValue
  private val baseFlavor_mf_map: Map[String, Seq[MF]] =
    rows.groupMap(_._3)(x => MF(x._1, x._2))

  // matches on last word in maker name
  private def makerLikeMatch(maker: String, matchSuffix: String): Boolean = {
    maker.split(' ') match{
      case ar: Array[_] if ar(ar.length -1) == matchSuffix => true
      case _                                               => false
    }
  }

  /*-------------------INTERFACE-------------------*/

  // Row   : 3-tuple mirrors file row data
  // RowSet: all transformations done using sets
  type Row    = (String,String,String)
  type RowSet = Set[Row]

  // analogous to index seek on IX_MakerFlavor_Flavor
  def getFlavorSet(flavors: Seq[String]): RowSet = {
    (flavor_bm_map withFilter (kv => flavors.contains(kv._1)) flatMap (kv =>
      kv._2 map (bm => (bm.maker, kv._1, bm.BaseFlavor)))).toSet
  }

  // analogous to clustered index seek on PK_Maker_Flavor (for finding suffix "Creamery")
  def getMakerLikeSet(likeString: String): RowSet = {
    maker_bf_map.keySet withFilter(key =>
      makerLikeMatch(key, likeString)) flatMap( key =>
        maker_bf_map(key) map(bf => (key,bf.flavor,bf.baseFlavor)))
  }

  // may be useful in future for other manipulations - can reverse deprecation
  @deprecated("inefficient for the data transformation - use getFlavorSet")
  def getBaseFlavorSet(baseFlavors: Seq[String]): RowSet = {
    (baseFlavors flatMap(bf =>
      baseFlavor_mf_map(bf) map(bf_value => (bf_value.maker,bf_value.flavor,bf)))).toSet
  }

  // using symbol aliases for set union and intersection
  def unionSets    (left: RowSet, right: RowSet): RowSet = left | right
  def intersectSets(left: RowSet, right: RowSet): RowSet = left & right
}

/*
    uses object IceCream to create actual results
 */
object IceCream_test extends App{
  import IceCream._

  IceCream  // invoke singleton constructor

  /*
      SOLUTION I
        each step mirrors the T-SQL execution plan
   */
  val flavor_set: RowSet =
    getFlavorSet(Seq[String]("Mint","Coffee","Vanilla"))

  // this pair simulates the pair in the UNION part of the T-SQL
  val creamery_set    : RowSet = getMakerLikeSet("Creamery")
  val flavor_set_union: RowSet = getFlavorSet(Seq[String]("Vanilla"))

  /* NOT USED as per T-SQL optimization: produces four rows, two of which not used
  val flavor_set_union_BF: RowSet = getBaseFlavorSet(Seq("Vanilla"))
  */
  // take the two sets above and create their union
  val union_set: RowSet = unionSets(creamery_set, flavor_set_union)

  // apply the equivalent of the T-SQL INTERSECT operator
  val intersection_set: RowSet = intersectSets(flavor_set, union_set)
    intersection_set foreach println; println()

  /*--------------------------------*/

  /*
      SOLUTION II
        the first solution may be preferred for its clarity/readability
        this one is more 'functional' style and compact - what I would normally write
        after developing and testing above to break the problem down
   */
  val queryResult: RowSet =
    getFlavorSet(Seq[String]("Mint","Coffee","Vanilla"))
      .& (getMakerLikeSet("Creamery")
        .| (getFlavorSet(Seq[String]("Vanilla"))))
  queryResult foreach println
}
