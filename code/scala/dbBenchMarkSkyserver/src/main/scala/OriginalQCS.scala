import com.github.tototoshi.csv.CSVWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.matching.Regex

object OriginalQCS {
  // Set Logger ... .
  Logger.getLogger("org").setLevel(Level.WARN)

  // Create a SparkSession.
  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .config("spark.sql.debug.maxToStringFields", "1000")
    .config("spark.driver.memory", "2g")
    .getOrCreate()

  // Values containing directory paths for the logs and tables folder.
  val DirLog: String = "/Users/eddy/Documents/study_github/2IMD00_seminar_datamanagement/output_logs/qcs_test/"
  val DirTable: String = "/Users/eddy/Documents/study_github/2IMD00_seminar_datamanagement/output_tables/qcs_test/"
//  val DirTable: String = "/Users/eddy/Documents/study_github/2IMD00_seminar_datamanagement/output_tables/"

  var attributeSet: Set[String] = Set()
  var attributeExtendedSet: Set[String] = Set()
  var joinClauseList: Set[List[String]] = Set()
//  var qcsExtended: ListBuffer[List[String]] = ListBuffer()
  var qcsWithQcsExtendedMap: Map[List[String], List[String]] = Map()

  def main(args: Array[String]): Unit = {

    val tablesList: List[String] = getListOfFiles(DirTable)
    val queryList: List[String] = getListOfFiles(DirLog)

//    for (i <- tablesList) {
//      println(i)
//    }

//    for (i <- queryList) {
//      println(i)
//    }

//    println(tablesList(1))
//    println(tablesList(1).getClass())

    // Extract the queries from the query log as a List of Strings.
//    val queries: List[String] = getQueryLog(queryList(0))
    val queries: List[String] = getQueryLog(queryList(2))

    // Import the tables and create or replace the views.
    createTempTableView(tablesList)
//    spark.sql("SHOW TABLES").show(26)

    // Executing the QCS ... .
    qcs(queries)

//    val qry_statement: String = queries(2)
//    val qry_statement: String = queries(4)
//    val qry_statement: String = queries(7)
//    val qry_statement: String = queries(9)
//    val qry_statement: String = queries(6)
//
//    val result: QueryExecution = spark.sql(qry_statement).queryExecution
//    println(qry_statement)
//    println(result.analyzed)
//
//    val aggregate = extractAggregate(result.analyzed)
//    val where = extractFilterCon(result.analyzed)
//    val groupBy = extractGroupByKey(result.analyzed)
//    val join = extractJoinTest(result.analyzed)
//    val qryAliases = extractQueryAlias(result.analyzed)
//
//    getAggregateAttrib(aggregate)
//    getWhereAttrib(where)
//    getGroupbyAttrib(groupBy)
//    getJoiningClause(join)
//    println(getTableAttribName(qryAliases))
//
//    println(qcsExtended.toList)

//    println("-------------------------------------------------------------------------------------------------------------------")

//    for (elem <- queries) {
//      val result: QueryExecution = spark.sql(elem).queryExecution
//
////      val aggregate = extractAggregate(result.analyzed)
//      val where = extractFilterCon(result.analyzed)
//      val groupBy = extractGroupByKey(result.analyzed)
//      val join = extractJoinTest((result.analyzed))
//      val qryAliases = extractQueryAlias(result.analyzed)
//
////      println(elem)
////      println(result.analyzed)
//
////      getAggregateAttrib(aggregate)
//
////      println(where)
////      println(groupBy)
////      println(join)
//
//      getWhereAndGroupbyAttrib(where)
//      getWhereAndGroupbyAttrib(groupBy)
//      getJoiningClause(join)
//
//      qcsResults += getTableAttribName(qryAliases)
//
////      println(attributeSet)
////      println(joinClauseList)
////      println(qcsResults)
//
//
////      println("-------------------------------------------------------------------------------------------------------------------")
//      attributeSet = attributeSet.empty
//      joinClauseList = joinClauseList.empty
//    }
//
//    qcsFrequency(qcsResults)


//    attributeSet.foreach(println)
//    println("-------------------------------------------------------------------------------------------------------------------")
//
//    val aggregate = extractAggregate(result.analyzed)
//    val whereClause = extractFilterCon(result.analyzed)
//    val groupBy = extractGroupByKey(result.analyzed)
//    val joinClause = extractJoinTest(result.analyzed)
//    val qryAlias = extractQueryAlias(result.analyzed)
//
//
//    println("Aggregate Predicate:\n" + aggregate)
//    println("\nWhere Predicate:\n" + whereClause)
//    println("\nGroup By Predicate:\n" + groupBy)
//    println("\nJoin Predicate:\n" + joinClause)
//    println("\nTable names:\n" + qryAlias)
//
//    println("###################################################################################################################")
  }

  /**
   * Iterate through a folder to get the paths of all the csv-files inside the folder.
   * @param dir The folder to iterate through.
   * @return List containing Strings of the paths of all the csv-files located in the dir.
   */
  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.getPath).toList
  }


  /**
   * Create temporary view or replace this view, when it already exists, based on the directory path of a csv file.
   * Extract the table name from the directory path, which is used to create the view.
   *
   * @param tables List of String which contains the directory path of the csv table files.
   */
  def createTempTableView(tables: List[String]) {
    for (i <- tables) {
      // Path is fixed, therefore you know place 8 is the filename for folder qcs_test.
      val fileName = i.split("/")(8)

      // File names always have extension '.csv' therefore endIndex is also fixed to 'length - 4'
      val tableName = fileName.substring(0, fileName.length - 4)

      // Create DataFrame based on the csv-file which contains a header as first line of the file.
      val df: DataFrame = spark.read.options(Map("header"->"true", "delimiter"->",")).csv(i)
      df.createOrReplaceTempView(tableName)
    }
  }

  /**
   * Extract all the query-statements from the query log, which is a csv file, and is returned as a List of Strings.
   * @param csvLog The path of where the query log is located.
   * @return List of Strings containing the query statements.
   */
  def getQueryLog(csvLog: String): List[String] = {
    val queryLog: DataFrame = spark.read.options(Map("header"->"true", "delimiter"->",")).csv(csvLog)

    queryLog.select("statement").rdd.map(r => r(0).asInstanceOf[String]).collect().toList
  }


  /**
   * Extract the aggregate predicate from the analyzed query plan.
   * @param plan Analyzed query plan.
   * @return
   */
  def extractAggregate(plan: LogicalPlan): Seq[Expression] = plan match {
    case a@Aggregate(groupingExpressions: Seq[Expression], aggregateExpressions: Seq[NamedExpression], child: LogicalPlan) =>
      aggregateExpressions ++ extractAggregate(child)
    case l: LeafNode =>
      Seq()
    case _ =>
      plan.children.flatMap(extractAggregate)
  }


  /**
   *
   * @param pl
   * @return
   */
  def extractGroupByKey(pl: LogicalPlan): Seq[Expression] = pl match {
    case a@Aggregate(groupingExpressions: Seq[Expression], aggregateExpressions: Seq[NamedExpression], child: LogicalPlan) =>
      groupingExpressions ++ extractGroupByKey(child)
    case l: LeafNode =>
      Seq()
    case _ =>
      pl.children.flatMap(extractGroupByKey)
  }

  /**
   *
   * @param pl
   * @return
   */
  def extractFilterCon(pl: LogicalPlan): Seq[Expression] = pl match {
    case j@Filter(condition: Expression, child: LogicalPlan) =>
      Seq(condition) ++ extractFilterCon(child)
    case l: LeafNode =>
      Seq()
    case _ =>
      pl.children.flatMap(extractFilterCon)
  }


  /**
   *
   * @param plan
   * @return
   */
  def extractJoinTest(plan: LogicalPlan): Seq[Expression] = plan match {
    case j@Join(left: LogicalPlan, right: LogicalPlan, joinType: JoinType, condition: Option[Expression], hint: JoinHint) =>
      Seq(condition.get) ++ extractJoinTest(left) ++ extractJoinTest(right)
    case l: LeafNode =>
      Seq()
    case _ =>
      plan.children.flatMap(extractJoinTest)
  }

  /**
   * Get the ... predicates.
   * @param plan Analyzed Query Plan
   * @return List
   */
  def extractQueryAlias(plan: LogicalPlan): Seq[Object] = plan match {
    case s@SubqueryAlias(identifier: AliasIdentifier, child: LogicalPlan) =>
      Seq(identifier.name) ++ extractQueryAlias(child)
//      Seq(identifier.name, extractQueryAlias(child))
//      Seq(extractQueryAlias(child)) ++ extractQueryAlias(child)
    case l: LeafNode =>
      Seq(l.output.toString())
//      Seq(l.toString())
    case _ =>
      plan.children.flatMap(extractQueryAlias)
  }


  /**
   *
   * @param aggregateList
   */
  def getAggregateAttrib(aggregateList: Seq[Expression]) {
    // Old Regexes to select every aggregate statement using findAllIn().
//    val aggregateAttribCntAvg: Regex = "(avg|count)\\((cast\\()?(\\w+\\#\\d+)".r
//    val aggregateAttribSum: Regex = "(sum)\\(cast\\(.+cast\\((\\w+\\#\\d+)".r
//    val aggregateAttribCntAka: Regex = "(count\\(1\\))\\s\\w+\\s(\\w+)\\#".r
//    val aggregateAttribCntAll: Regex = "count\\(1\\)\\sAS\\scount\\(1\\)\\#".r

    // New Regexes for findFirstMatchIn():
//    val aggregateAttribCntAndCntall: Regex = "count\\(1?(\\w+\\#\\d+)?\\)\\sas\\s(count\\(1?\\w*\\))".r
    val aggregateAttribCntall: Regex = "count\\(1\\)\\sas\\s.+\\#".r
//    val aggregateAttribCntAka: Regex = "count\\(1\\)\\sas\\s(\\w+)\\#".r
    val aggregateAttribCnt: Regex = "count\\((\\w+\\#\\d+)?\\)\\sas\\s.+\\#".r
    val aggregateAttribAvg: Regex = "avg\\(cast\\((\\w+\\#\\d+)".r
    val aggregateAttribSum: Regex = "sum\\(cast\\(.+cast\\((\\w+\\#\\d+)".r

//    println(aggregateList)

    // When the aggregateList is empty the synthetic query will get the statement select *.
    if (aggregateList.isEmpty) {
      attributeExtendedSet += "ALL;select"
    }

    /* Using Regexes to find the first match of a count, count(*), avg or sum. */
    // Find the first match of a single count or count(*) statement.
//    aggregateAttribCntAndCntall.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach {
//      m =>
////            println("Match: " + m)
//        if (m.group(1) != null) {
////          println("No Loop Count, when not NULL Group 1: " + m.group(1))
//          attributeExtendedSet += m.group(1).toLowerCase() + ";count"
//        } else {
////          println("No Loop Count, when NULL Group 1: " + m.group(1))
//          attributeExtendedSet += "ALL;count(*)"
//        }
//    }
    aggregateAttribCntall.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach {
      m =>
        //            println("Match: " + m)
        attributeExtendedSet += "ALL;count(*)"
    }


    // Find the first match of a count which will be shown with an alias.
//    aggregateAttribCntAka.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach {
    aggregateAttribCnt.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach {
      m =>
//        println("Group1: " + m.group(1).toLowerCase())
//        println("Group2: " + m.group(2).toLowerCase())
//        println("No Loop CountAKA Match: " + m)
//        println("No Loop CountAKA Group 1: " + m.group(1))
//        attributeExtendedSet += m.group(1).toLowerCase() + "_ALIAS;count(*)"
        attributeExtendedSet += m.group(1).toLowerCase() + ";count"
    }


    // Find the first match of an Average statement.
    aggregateAttribAvg.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach {
      m =>
//        println("No Loop Average Match: " + m)
//        println("No Loop Average Group 1: " + m.group(1))
        attributeExtendedSet += m.group(1).toLowerCase() + ";avg"
    }

    // Find the first match of a summation statement.
    aggregateAttribSum.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach{
      m =>
//        println("No Loop Sum Match: " + m)
//        println("No Loop Sum Group 1: " + m.group(1) + ";sum")
        attributeExtendedSet += m.group(1).toLowerCase() + ";sum"
    }


    // Old for loop to get every aggregate statement from the query plan, but by doing this the synthetic would look
    // almost identical to the original query.
//    aggregateList.foreach {
//      i =>
//        println(i)
//
//        aggregateAttribCntAndCntall.findFirstMatchIn(i.toString().toLowerCase()).foreach {
//          m =>
//            println("Match: " + m)
//            if (m.group(1) != null) {
//              println("Group 1: " + m.group(1))
//              attributeExtendedSet += m.group(1).toLowerCase() + ";count"
//            } else {
//              println("Group 1: " + m.group(1))
//              attributeExtendedSet += "ALL;count(*)"
//            }
//        }
//
////        aggregateAttribCntAka.findAllIn(i.toString()).matchData foreach {
//        aggregateAttribCntAka.findFirstMatchIn(i.toString().toLowerCase()).foreach {
//          m =>
//            println("Match: " + m)
//            println("Group 1: " + m.group(1))
//            attributeExtendedSet += m.group(1).toLowerCase() + "_ALIAS;count(*)"
//        }
//
////        aggregateAttribCntAvg.findAllIn(i.toString()).matchData foreach{
//        aggregateAttribAvg.findFirstMatchIn(i.toString().toLowerCase()).foreach{
//          m =>
//            println(m.group(3).toLowerCase() + ";" + m.group(1).toLowerCase())
//            attributeExtendedSet += m.group(3).toLowerCase() + ";" + m.group(1).toLowerCase()
//            attributeSet += m.group(3).toLowerCase() + ";" + m.group(1).toLowerCase()
//            attributeExtendedSet += m.group(3).toLowerCase() + ";" + m.group(1).toLowerCase()
//            println("Match: " + m)
//            println("Group 1: " + m.group(1))
//
//        }
//
////        aggregateAttribSum.findAllIn(i.toString()).matchData foreach{
//        aggregateAttribSum.findFirstMatchIn(i.toString().toLowerCase()).foreach{
//          m =>
//            println(m.group(2) + ";" + m.group(1))
//            attributeExtendedSet += m.group(2).toLowerCase() + ";" + m.group(1).toLowerCase()
//            attributeSet += m.group(2).toLowerCase() + ";" + m.group(1).toLowerCase()
//            attributeExtendedSet += m.group(2).toLowerCase() + ";" + m.group(1).toLowerCase()
//            println("Match: " + m)
//            println("Group 1: " + m.group(1) + ";sum")
//        }
//    }
  }


  /**
   *
   * @param whereList
   */
  def getWhereAttrib(whereList: Seq[Expression]) {
    val unresolvedAttrib: Regex = "(\\w+\\#\\d+)(\\sas\\s\\w+\\)\\s|\\s)(\\=|\\<\\>|\\>\\=|\\<\\=|\\<|\\>|\\w+)".r
    val newUnresolvedAttrib: Regex = "(\\w+\\#\\d+)(\\s|\\sas\\s\\w+\\)\\s)(\\=|\\<\\>|\\>\\=|\\<\\=|\\<|\\>|\\%|LIKE)\\s(cast\\()?(.?\\d+\\.?\\d+|\\w+)".r

//    println(whereList)

    whereList.foreach{
      i =>
//        println(i)
//        unresolvedAttrib.findAllIn(i.toString()).matchData foreach{
//          m =>
////            attributeSet += m.toString()
//            println(m)
//        }

//        unresolvedAttrib.findAllIn(i.toString()).matchData foreach{
        newUnresolvedAttrib.findAllIn(i.toString()).matchData foreach{
          m =>
            // Includes the specific where clause sign. (=, <, >, >=, <=, <>, %, LIKE, IN)
//            attributeSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase()
            attributeSet += m.group(1).toLowerCase()
//            attributeExtendedSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase() + "_" + m.group(5).toLowerCase()

//            println(m.group(5) + " => " + m.group(5).matches("\\-?\\d+\\.?\\d*"))

            // Check if constraint contains letters or digits, if it contains letters put between ''.
//            if (m.group(5).forall(_.isLetter)) {
//              attributeExtendedSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase() + "_'" + m.group(5).toLowerCase() + "'"
//            } else {
//              attributeExtendedSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase() + "_" + m.group(5).toLowerCase()
//            }

            if (!m.group(5).matches("\\-?\\d+\\.?\\d*")) {
              attributeExtendedSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase() + "_'" + m.group(5).toLowerCase() + "'"
            } else {
              attributeExtendedSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase() + "_" + m.group(5).toLowerCase()
            }
        }
    }
  }


  /**
   *
   * @param groupByList
   */
  def getGroupbyAttrib(groupByList: Seq[Expression]) {
    val unresolvedGroupbyAttrib: Regex = "(\\w+\\#\\d+)".r

    groupByList.foreach{
      i =>
//        println(i)
        unresolvedGroupbyAttrib.findAllIn(i.toString()).matchData foreach{
          m =>
            attributeSet += m.toString().toLowerCase()
            attributeExtendedSet += m.toString().toLowerCase() + ";groupBy"
//            println(m + ";groupBy")
        }
    }
  }


  /**
   *
   * @param joinList
   */
  def getJoiningClause(joinList: Seq[Expression]) {
    val joiningAttrib: Regex = "(\\w+#\\d+)".r

    joinList.foreach{
      i =>
        val joinAttribList = new ListBuffer[String]()
//        println(i)
        joiningAttrib.findAllIn(i.toString()).matchData foreach{
          m => joinAttribList += m.toString().toLowerCase()
        }
//        println(joinAttribList.toList)
        joinClauseList += joinAttribList.toList
//        println(joinClauseList)
    }
  }


  /**
   *
   * @param unresolvedColumnList
   */
//  def getTableAttribName(unresolvedColumnList: Seq[Object]) {
  def getTableAttribName(unresolvedColumnList: Seq[Object]): List[String] = {
    var tableName: String = ""
    var tableColumnMap = Map[String, Object]()
    val attribList: ListBuffer[String] = ListBuffer()
    val attribExtendedList: ListBuffer[String] = ListBuffer()

    // Extract the Table name and the unresolved column list and add these to a Map, Key: Table name and Value: Unresolved column list
    if (unresolvedColumnList.length < 3) { // When the query only retrieves data from one table.
      // Extract the table name
      tableName = unresolvedColumnList(0).toString
      attribExtendedList += tableName.toLowerCase() + ";from"

      // Add the table name as key and the unresolved column list as value to the Map tableColumnMap.
      tableColumnMap += (tableName -> unresolvedColumnList(1))

      //      println("Table Name: " + qryAliases(0).toString)
      //      println("Column Names: " + qryAliases(1))
    } else { // When the query contains join statements.
      unresolvedColumnList.foreach {
        i =>
          /* Starting from position 1 every third position is the table name and every third position from starting
             position 2 is the unresolved column list which should be added as value to the Map tableColumnMap. */
          if (unresolvedColumnList.indexOf(i) < 3 && (unresolvedColumnList.indexOf(i) % 3) == 1) {
            tableName = i.toString
            attribExtendedList += tableName.toLowerCase() + ";from"
          } else if (unresolvedColumnList.indexOf(i) > 3 && (unresolvedColumnList.indexOf(i) % 3) == 1) {
//            println("Table Name: " + i)
            tableName = i.toString
          } else if ((unresolvedColumnList.indexOf(i) > 0 && unresolvedColumnList.indexOf(i) < 3) && ((unresolvedColumnList.indexOf(i) % 3) % 2) == 0) {
//            println("Column Names: " + i.getClass)
            tableColumnMap += (tableName -> i)
          } else if (unresolvedColumnList.indexOf(i) > 3 && (unresolvedColumnList.indexOf(i) % 3) == 2) {
//            println("Column Names: " + i.getClass)
            tableColumnMap += (tableName -> i)
          }
      }
    }

    // Testing, see(print) the content of the map.
//    tableColumnMap foreach {
//      case (key, value) => println (key + " --> " + value)
//    }

    attributeSet.foreach {
      i =>
        // Create new regex based on unresolved column name.
        val attribRegx: Regex = new Regex(i)

        // Iterate through map and find matching unresolved column name of table.
        for ((table, columnList) <- tableColumnMap) {
          if (!attribRegx.findFirstIn(columnList.toString.toLowerCase()).isEmpty) {
            val completeAttribName: String = table + "." + i.split("#")(0)
//            println(completeAttribName)
            attribList += completeAttribName
          }
        }
    }

    attributeExtendedSet.foreach {
      i =>
        // Create new regex based on unresolved column name.
        val splittedString: Array[String] = i.split(";")
        val attribRegx: Regex = new Regex(splittedString(0))
        val aggregateCntSlctAllRegx: Regex = ".+\\;(select|count\\(\\*\\))".r

        // Add the specific aggregate clause of select * or count(*) and add to the QCS Extended List.
        aggregateCntSlctAllRegx.findAllIn(i.toString()).matchData foreach {
          m =>
//            println("Pickle Rick: " + m.group(0))
            attribExtendedList += m.group(0)
        }

        // Iterate through map and find matching unresolved column name of table.
        for ((table, columnList) <- tableColumnMap) {
          if (!attribRegx.findFirstIn(columnList.toString.toLowerCase()).isEmpty) {
            val completeAttribName: String = table + "." + i.split("#")(0)
//            println(completeAttribName)
            attribExtendedList += completeAttribName + ";" + splittedString(1)
          }
        }
    }

//    println(joinClauseList)

    joinClauseList.foreach {
      i =>
        // Create new regexes based on unresolved column names.
        val joinTable1Regex: Regex = new Regex(i(0))
        val joinTable2Regex: Regex = new Regex(i(1))

        var joinColumnName1: String = ""
        var joinColumnName2: String = ""

        // Iterate through Map and find matching unresolved column name of the table.
        for ((table, columnList) <- tableColumnMap) {
          if (!joinTable1Regex.findFirstIn(columnList.toString.toLowerCase()).isEmpty) {
            joinColumnName1 = table + "." + i(0).split("#")(0)
//            println(joinColumnName1)
          }

          if (!joinTable2Regex.findFirstIn(columnList.toString.toLowerCase()).isEmpty) {
            joinColumnName2 = table + "." + i(1).split("#")(0)
//            println(joinColumnName2)
          }
        }

        val joinClause: String = joinColumnName1 + " = " + joinColumnName2

//        println(joinClause + ";join")
//        println(List(joinColumnName1 + " = " + joinColumnName2))
        attribList += joinClause
        attribExtendedList += joinClause + ";join"
    }

//    println(attribList.toList)
//  println(attribExtendedList)

    /*
    TESTING FOR MAPPING ORIGINAL QCS WITH EXTENDED QCS.
     */
    qcsWithQcsExtendedMap += (Random.shuffle(attribExtendedList.toList) -> attribList.toList)

    attribList.toList
  }


  /**
   *
   * @param qcsList
   */
  def qcsFrequency(qcsList: ListBuffer[List[String]]): Map[List[String], Int] = {
    var qcsMap: Map[List[String], Int] = Map()
    var tmpList: List[String] = List()

//    qcsList.foreach {
//      i => println(i.sorted)
//    }

//    qcsList.foreach(println)

    qcsMap += (qcsList(0).sorted -> 1)

    // Start printing from position 1 until end of List instead of starting from position 0.
    for (i <- 1 until qcsList.length) {
      tmpList = qcsList(i).sorted

      // If the QCS List of attributes is not in the Map qcsMap as a Key add this List as a Key with the value of 1, if
      // the List is already in the Map qcsMap as a key. Increment the value corresponding to that key(List) with 1.
      if (qcsMap.contains(tmpList)) {
        var newFreq: Int = qcsMap(tmpList)
        newFreq += 1

        qcsMap = qcsMap + (tmpList -> newFreq)
      } else {
        qcsMap += (tmpList -> 1)
      }
    }

    // Testing, see(print) the content of the map.
//    println("-------------------------------------------------------------------------------------------------------------------")
//    qcsMap foreach {
//      case (key, value) => println (key + " --> " + value)
//    }

    qcsMap
  }


  /**
   * For the csv writer I made use of: https://github.com/tototoshi/scala-csv.
   *
   * @param totalQCSMap
   */
  def qcsToCsv(totalQCSMap: Map[List[String], Int]): Unit = {
    val outputCsv = new File("/Users/eddy/Documents/study_github/2IMD00_seminar_datamanagement/output_qcs_test.csv")

    val writer = CSVWriter.open(outputCsv)

    totalQCSMap foreach {
      case (key, value) =>
        writer.writeRow(List(value, key.sortBy(_.length).mkString(", ")))
    }

    writer.close()
  }


  /**
   * For the csv writer I made use of: https://github.com/tototoshi/scala-csv.
   *
   * @param totalQCSExtendedMap
   */
  def qcsExtendedToCsv(totalQCSExtendedMap: Map[List[String], List[String]]): Unit = {
    val outputCsv = new File("/Users/eddy/Documents/study_github/2IMD00_seminar_datamanagement/output_qcs_extended_test.csv")

    val writer = CSVWriter.open(outputCsv)

    totalQCSExtendedMap foreach {
      case (key, value) =>
        writer.writeRow(List(key.sortBy(_.length).mkString(", "), value.mkString(", ")))
    }

    writer.close()
  }


  /**
   *
   * @param qcsMap
   * @return
   */
  def getQcsQueries(qcsMap: Map[List[String], Int], prob: Int): ListBuffer[List[String]] = {
    val qryQCS: ListBuffer[List[String]] = ListBuffer()

// Testing, see(print) the content of the map.
//    println("-------------------------------------------------------------------------------------------------------------------")
//    qcsWithQcsExtendedMap foreach {
//      case (qcsExtended, originalQcs) =>
//        println (qcsExtended + " --> " + originalQcs)
//    }

    qcsMap foreach {
      case (qcsSet, frequency) =>
        if (frequency > prob) {
          qcsWithQcsExtendedMap foreach {
            case (qcsExtended, originalQcsSet) =>
              if (originalQcsSet.sorted.equals(qcsSet.sorted)) {
//                println(qcsExtended.sorted)
                qryQCS += qcsExtended
              }
          }
        }
    }

    qryQCS
  }


  /**
   *
   * @param QCSMap
   */
  def generateQueries(QCSExtendedMap: ListBuffer[List[String]]): Unit = {
    val generatedQueriesList: ListBuffer[List[String]] = ListBuffer()
    val queriesCsv = new File("/Users/eddy/Documents/study_github/2IMD00_seminar_datamanagement/output_qcs_queries_test.csv")
    val writer = CSVWriter.open(queriesCsv)

    // Different Regular Expressions, to specifically create a part of a query.
    val selectRegxr: Regex = "(\\w+.?\\w+)\\;(select|avg|sum|count\\(\\*\\)|count)".r
    val fromRegxr: Regex = "(\\w+)\\;(from)".r
    val joiningRegxr: Regex = "(\\w+\\.\\w+.\\s\\=\\s\\w+\\.\\w+)\\;(join)".r
//    val whereRegxr: Regex = "(\\w+\\.\\w+)\\;where\\_(\\=|\\<\\>|\\>\\=|\\<\\=|\\<|\\>|\\w+)".r
//    val whereRegxr: Regex = "(\\w+\\.\\w+)\\;where\\_(\\=|\\<\\>|\\>\\=|\\<\\=|\\<|\\>|\\w+)\\_(\\-?\\d\\.?\\d+|\\w+)".r
    val whereRegxr: Regex = "(\\w+\\.\\w+)\\;where\\_(\\=|\\<\\>|\\>\\=|\\<\\=|\\<|\\>|\\w+)\\_(\\-?\\d\\.?\\d+|\\'?\\w+\\'?)".r
    val groupByRegxr: Regex = "(\\w+\\.\\w+)\\;groupBy".r

    // Declaring the different parts of the Query and initializing them as empty Strings.
    var selectPart: String = ""
    var fromPart: String = ""
    var joiningPart: String = ""
    var wherePart: String = ""
    var groupByPart: String = ""

    QCSExtendedMap foreach {
      qryBase =>
        selectPart = "select "
//        fromPart = ""
        joiningPart = ""
        wherePart = "where "
        groupByPart = "group by "

        // When there is no 'GROUP BY' clause reset the String groupByPart to an empty String.
        if (!qryBase.toString().contains("groupBy")) {
          groupByPart = ""
        }

        // When there is no 'GROUP BY' clause reset the String groupByPart to an empty String.
        if (!qryBase.toString().contains("where")) {
          wherePart = ""
        }

        // Create the 'FROM' statement based on the Regex fromRegxr.
        fromRegxr.findFirstMatchIn(qryBase.toString()).foreach {
          m =>
            fromPart = m.group(2) + " " + m.group(1) + " "
        }

        // Loop through every QCS List.
        for (i <- 0 until qryBase.length) {

//            println(key(i))

          // Create the 'SELECT' statement based on the Regex selectRegxr.
          selectRegxr.findAllIn(qryBase(i)).matchData foreach {
            m =>
              if (m.group(2) == "avg" | m.group(2) == "sum" | m.group(2) == "count") {
//                println("Pickle Rick: " + m.group(2) + "(" + m.group(1) + ")")
                selectPart += m.group(2) + "(" + m.group(1) + "), "
              }

              if (m.group(2) == "count(*)" && m.group(1) == "ALL") {
//                  println("Pickle Rick: " + m.group(2))
                selectPart += m.group(2) + ", "
              }

              if (m.group(2) == "select") {
//                println("Pickle Rick: " + " * ")
                selectPart += "*   "
              }

          }

          // Create the 'JOIN' statements based on the Regex joiningRegxr.
          joiningRegxr.findAllIn(qryBase(i)).matchData foreach {
            m =>
              val tmpTable1: String = m.group(1).split("\\.")(0).toLowerCase()
              val tmpTable2: String = m.group(1).split(" = ")(1).split("\\.")(0)
              var joiningTable: String = ""

              if (fromPart.split("\\s")(1).toLowerCase() == tmpTable1.toLowerCase()) {
                joiningTable = tmpTable2
              } else {
                joiningTable = tmpTable1
              }

              joiningPart += m.group(2) + " " + joiningTable + " on " + m.group(1) + " "
          }

          // Create the 'WHERE' statement based on the Regex whereRegxr.
          whereRegxr.findAllIn(qryBase(i)).matchData foreach {
            m =>
//                wherePart += m.group(1) + " " + m.group(2) + " and "
              wherePart += m.group(1) + " " + m.group(2) + " " + m.group(3) + " and "

          }

          // Create the 'GROUP BY' statements based on the Regex groupByRegxr.
          groupByRegxr.findAllIn(qryBase(i)).matchData foreach {
            m =>
              groupByPart += m.group(1)  + ", "
          }
        }

        // Add the whole query to the List generatedQueriesList.
        generatedQueriesList += List(selectPart.dropRight(2) + " " + fromPart + joiningPart + wherePart.dropRight(5) + " " + groupByPart.dropRight(2))

//          println(selectPart.dropRight(2) + " " + fromPart + joiningPart + wherePart.dropRight(5) + " " + groupByPart.dropRight(2))
//          println("---------------------------------------------------------------------------------------------------------\n")
    }

//    println(generatedQueriesList)

    // Write queries inside the List generatedQueriesList to a csv file.
    generatedQueriesList.foreach {
      qry =>
        writer.writeRow(qry)
    }

    writer.close()
  }


  /**
   *
   * @param queries
   */
  def qcs(queries: List[String]) {
    val qcsResults: ListBuffer[List[String]] = ListBuffer()

    for (i <- queries) {
      println(i)

      val result: QueryExecution = spark.sql(i).queryExecution
//      println(result.analyzed)

      val aggregateClause = extractAggregate(result.analyzed)
      val whereClause = extractFilterCon(result.analyzed)
      val groupByClause = extractGroupByKey(result.analyzed)
      val joinClause = extractJoinTest(result.analyzed)
      val qryAliases = extractQueryAlias(result.analyzed)
//
//      println("-------------------------------------------------------------------------------------------------------------------")
//
//      println("\nWhere Predicate:\n" + whereClause)
//      println("\nGroup By Predicate:\n" + groupByClause)
//      println("\nJoin Predicate:\n" + joinClause)
//      println("\nTable names:\n" + qryAlias)
//
//      println("###################################################################################################################")

      getAggregateAttrib(aggregateClause)
      getWhereAttrib(whereClause)
      getGroupbyAttrib(groupByClause)
      getJoiningClause(joinClause)

      qcsResults  += getTableAttribName(qryAliases)

//      println(attributeSet)
//      println(joinClauseList)
//      println(qcsResults)
//
//      println("-------------------------------------------------------------------------------------------------------------------")

      // Clear the Sets for new iteration.
      attributeSet = attributeSet.empty
      attributeExtendedSet = attributeExtendedSet.empty
      joinClauseList = joinClauseList.empty
    }
//    println(qcsResults)

    val qcsMap: Map[List[String], Int] = qcsFrequency(qcsResults)
//    qcsFrequency(qcsResults)

    qcsToCsv(qcsMap)
    qcsExtendedToCsv(qcsWithQcsExtendedMap)

//    generateQueries(qcsMap)

//    val qcsWithQryBase: ListBuffer[List[String]] = getQcsQueries(qcsMap, 24)
    val qcsWithQryBase: ListBuffer[List[String]] = getQcsQueries(qcsMap, 0)

    generateQueries(qcsWithQryBase)

//    val testList: ListBuffer[List[String]] = ListBuffer()
//
//    qcsExtendedMap foreach {
//      case (qcsSet, frequency) =>
//        if (frequency > 1) {
//          qcsWithExtendedQcsMap foreach {
//            case (qcsExtended, originalQcsSet) =>
//              if (originalQcsSet.sorted.equals(qcsSet.sorted)) {
////                println(qcsExtended.sorted)
//                testList += qcsExtended
//              }
//          }
//        }
//    }
//
//    println(testList.size)

  }
}
