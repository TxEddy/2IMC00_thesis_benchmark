import com.github.tototoshi.csv.CSVWriter

import java.io.{BufferedWriter, File, FileWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LeafNode, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{StringType, StructField, MetadataBuilder}

import scala.collection.immutable.ListSet
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import scala.util.Random
import scala.util.control.Breaks.break

object QCS {
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
  val DirLog: String = "/Users/eddy/Documents/study_github/2IMC00_thesis_benchmark/output_logs/qcs_test/"
  val DirTable: String = "/Users/eddy/Documents/study_github/2IMC00_thesis_benchmark/output_tables/qcs_test/"
//  val DirTable: String = "/Users/eddy/Documents/study_github/2IMC00_thesis_benchmark/output_tables/"

  var attributeSet: Set[String] = Set()
  var attributeExtendedSet: Set[String] = Set()
//  var joinClauseList: Set[List[String]] = Set()
  var joinClauseList: ListBuffer[List[String]] = ListBuffer()
  val qcsExtended: ListBuffer[List[String]] = ListBuffer()

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
//    val queries: List[String] = getQueryLog(queryList(4)) // New log 2020
    val queries: List[String] = getQueryLog(queryList(3)) // Generated Queries 3(z_log_generated), 5(a_generated_qry_test)

    // Import the tables and create or replace the views.
    createTempTableView(tablesList)
//    spark.sql("SHOW TABLES").show(45)
//    getTableSchema(tablesList)


    // Executing the QCS ... .
    qcs(queries)


    // Execute query which retrieves data based QCS to calculate correlations.
//    getQueriesCsv("qcsAttributesOutput")
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
      // Path is fixed, therefore you know place ... is the filename for the folder output_tables.
//      val fileName = i.split("/")(7)

      // Path is fixed, therefore you know place 8 is the filename for folder qcs_test.
      val fileName = i.split("/")(8)

      // File names always have extension '.csv' therefore endIndex is also fixed to 'length - 4'
      val tableName = fileName.substring(0, fileName.length - 4)

      // Create DataFrame based on the csv-file which contains a header as first line of the file.
      val df: DataFrame = spark.read.options(Map("inferSchema"->"true", "header"->"true", "delimiter"->",")).csv(i)
//      val df: DataFrame = spark.read.options(Map("header"->"true", "delimiter"->",")).csv(i)
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

  def getTableSchema(tables: List[String]): Unit = {
    //    val testQry: String = "select * from "
    //    spark.sql("select * from " + "zoospec").printSchema()
//    val tableName = "zoospec"
//    val jsonFile = new File(s"/Users/eddy/Documents/study_github/2IMC00_thesis_benchmark/output_schema/$tableName.json")
//    val bw = new BufferedWriter(new FileWriter(jsonFile))

//    println(spark.sql(s"select * from $tableName").schema.prettyJson)
//    bw.write(spark.sql(s"select * from $tableName").schema.prettyJson)
//    bw.close()
//    println(spark.sql(s"select * from $tableName").schema.fields.foreach(println))
//    println(spark.sql(s"select * from $tableName").schema.fields)

//    println(spark.sql(s"select * from $tableName").schema.add(StructField("Test", StringType, true, new MetadataBuilder().putString("description", "primary key").build())).prettyJson)

//    for (i <- 0 until spark.sql(s"select * from $tableName").schema.length) {
//      println(spark.sql(s"select * from $tableName").schema.fields(i).name + ", " + spark.sql(s"select * from $tableName").schema.fields(i).dataType.typeName)
//    }

    for (i <- tables) {
      val tableName = i.split("/")(8).substring(0, i.split("/")(8).length - 4)
      val txtFile = new File(s"/Users/eddy/Documents/study_github/2IMC00_thesis_benchmark/output_schema/$tableName.txt")
      val txtBufferedWriter = new BufferedWriter(new FileWriter(txtFile))
//      txtBufferedWriter.write(spark.sql(s"select * from $tableName").schema.prettyJson.toLowerCase())
      txtBufferedWriter.write(s"CREATE TABLE $tableName (\n")

      for (j <- 0 until spark.sql(s"select * from $tableName").schema.length) {
        txtBufferedWriter.write(spark.sql(s"select * from $tableName").schema.fields(j).name.toLowerCase() + " " + spark.sql(s"select * from $tableName").schema.fields(j).dataType.typeName + ",\n")
      }
      txtBufferedWriter.write(");")
      txtBufferedWriter.close()
    }
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
    case f@Filter(condition: Expression, child: LogicalPlan) =>
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
   * Labelling the attributes with the correct aggregate statement using Regular Expressions.
   * @param aggregateList List with attributes stated in an aggregation statement.
   */
  def getAggregateAttrib(aggregateList: Seq[Expression]) {
    // New Regexes for findFirstMatchIn():
    val aggregateAttribCntall: Regex = "count\\(1\\)\\sas\\s.+\\#".r
//    val aggregateAttribCnt: Regex = "count\\((\\w+\\#\\d+)?\\)\\sas\\s.+\\#".r
    val aggregateAttribCnt: Regex = "count\\((?:distinct\\s)?(\\w+\\#\\d+\\w?)\\)\\sas\\s".r
    val aggregateAttribAvg: Regex = "avg\\((\\w+\\#\\d+)\\)".r
    val aggregateAttribSum: Regex = "sum\\(+cast.*\\((\\w+\\#\\d+)".r

    val aggregateAttribString = aggregateList.toString().toLowerCase()

//    println(aggregateList)

    // When the aggregateList is empty the synthetic query will get the statement select *.
    if (aggregateList.isEmpty || (aggregateAttribCntall.findFirstMatchIn(aggregateAttribString).isEmpty && aggregateAttribCnt.findFirstMatchIn(aggregateAttribString).isEmpty && aggregateAttribAvg.findFirstMatchIn(aggregateAttribString).isEmpty && aggregateAttribSum.findFirstMatchIn(aggregateAttribString).isEmpty)) {
      attributeExtendedSet += "ALL;select"
      attributeSet += "ALL;select"
    }



//    aggregateAttribCntall.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach {
    aggregateAttribCntall.findFirstMatchIn(aggregateAttribString).foreach {
      m =>
//        println("Match: " + m)
        attributeExtendedSet += "ALL;count(*)"
        attributeSet += "ALL;count(*)"
    }


    // Find the first match of a count which will be shown with an alias.
//    aggregateAttribCnt.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach {
    aggregateAttribCnt.findFirstMatchIn(aggregateAttribString).foreach {
      m =>
//        println("Group1: " + m.group(1).toLowerCase())
//        println("Group2: " + m.group(2).toLowerCase())
//        println("No Loop CountAKA Match: " + m)
//        println("No Loop CountAKA Group 1: " + m.group(1))
//        attributeExtendedSet += m.group(1).toLowerCase() + "_ALIAS;count(*)"
        attributeExtendedSet += m.group(1).toLowerCase() + ";count"
        attributeSet += m.group(1).toLowerCase() + ";count"
    }


    // Find the first match of an Average statement.
//    aggregateAttribAvg.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach {
    aggregateAttribAvg.findFirstMatchIn(aggregateAttribString).foreach {
      m =>
//        println("No Loop Average Match: " + m)
//        println("No Loop Average Group 1: " + m.group(1))
        attributeExtendedSet += m.group(1).toLowerCase() + ";avg"
        attributeSet += m.group(1).toLowerCase() + ";avg"
    }

    // Find the first match of a summation statement.
//    aggregateAttribSum.findFirstMatchIn(aggregateList.toString().toLowerCase()).foreach{
    aggregateAttribSum.findFirstMatchIn(aggregateAttribString).foreach{
      m =>
//        println("No Loop Sum Match: " + m)
//        println("No Loop Sum Group 1: " + m.group(1) + ";sum")
        attributeExtendedSet += m.group(1).toLowerCase() + ";sum"
        attributeSet += m.group(1).toLowerCase() + ";sum"
    }
  }


  /**
   * Labelling the attributes with the correct WHERE/condition statement using Regular Expressions.
   * @param whereList List with attributes stated in a WHERE statement.
   */
  def getWhereAttrib(whereList: Seq[Expression]) {
    val UnresolvedAttrib: Regex = "(\\w+\\#\\d+)(\\s|\\sas\\s\\w+\\)\\s)(\\=|\\<\\>|\\>\\=|\\<\\=|\\<|\\>|\\%|LIKE)\\s(cast\\()?(.?\\d+\\.?\\d+|\\w+)".r

//    println(whereList)

    whereList.foreach{
      i =>
//        println(i)

        UnresolvedAttrib.findAllIn(i.toString()).matchData foreach{
          m =>
            // Includes the specific where clause sign. (=, <, >, >=, <=, <>, %, LIKE, IN)
//            attributeSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase()

//            println(m.group(5) + " => " + m.group(5).matches("\\-?\\d+\\.?\\d*"))

            if (!m.group(5).matches("\\-?\\d+\\.?\\d*")) {
              attributeExtendedSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase() + "_'" + m.group(5).toLowerCase() + "'"
              attributeSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase()
            } else {
              attributeExtendedSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase() + "_" + m.group(5).toLowerCase()
              attributeSet += m.group(1).toLowerCase() +";where_" +  m.group(3).toLowerCase()
            }
        }
    }
  }


  /**
   * Labelling the attributes with ";groupBy" statement using Regular Expressions.
   * @param groupByList List with attributes stated in the GROUP BY statement.
   */
  def getGroupbyAttrib(groupByList: Seq[Expression]) {
    val unresolvedGroupbyAttrib: Regex = "(\\w+\\#\\d+)".r

    groupByList.foreach{
      i =>
//        println(i)
        unresolvedGroupbyAttrib.findAllIn(i.toString()).matchData foreach{
          m =>
            attributeSet += m.toString().toLowerCase() + ";groupBy"
            attributeExtendedSet += m.toString().toLowerCase() + ";groupBy"
//            println(m + ";groupBy")
        }
    }
  }


  /**
   * Get the attributes stated in a JOIN statement.
   * @param joinList List with attributes which were stated in the JOIN statement.
   */
  def getJoiningClause(joinList: Seq[Expression]) {
    val joiningAttrib: Regex = "(\\w+#\\d+\\w?)".r
//    println("Joining List:")
//    joinList.foreach {
//      joinStatement =>
//        println(joinStatement)
//    }

    joinList.foreach{
      i =>
        val joinAttribList = new ListBuffer[String]()
//        println(i)
        joiningAttrib.findAllIn(i.toString()).matchData foreach{
          m => joinAttribList += m.toString().toLowerCase()
//            println(m)
        }

//        println(joinAttribList.toList)
        joinClauseList += joinAttribList.toList
//        println(joinClauseList)
    }
  }

  def testQueryAlias(unresolvedColumnList: Seq[Object]) {
    var tableName: String = ""
    var tableColumnMap = Map[String, Object]()
    val attribList: ListBuffer[String] = ListBuffer()
    val attribExtendedList: ListBuffer[String] = ListBuffer()
    var isFromTable: Boolean = true

    println("Query Aliases:")
    for (i <- unresolvedColumnList.indices) {
      if (unresolvedColumnList(i).toString.contains("List")) {
        tableName = unresolvedColumnList(i - 1).toString.toLowerCase()

        if (isFromTable) {
          attribList += tableName + ";from"
          attribExtendedList += tableName + ";from"
//          println("From: " + tableName)
          isFromTable = false
        }

        // IF-statement when self-joining.
        if (!tableColumnMap.contains(tableName)) {
          tableColumnMap += (tableName -> unresolvedColumnList(i).toString.toLowerCase())
        }
//        println("The unresolved column list:")
//        println(unresolvedColumnList(i))
//        println("The previous value is: " + unresolvedColumnList(i - 1))
//        println(unresolvedColumnList(i - 1) + ": " + unresolvedColumnList(i))
      }
    }

    // Testing, see(print) the content of the map.
//    println("Map content:")
//    tableColumnMap foreach {
//      case (key, value) => println (key + " --> " + value)
//    }

//    unresolvedColumnList.foreach {
//      i =>
////        println(i)
////        println("Is type of: " + i.getClass)
//        if (i.toString.contains("List")) {
//          println("The unresolved column list:")
//          println(i)
//        }
//
//    }

    // Extract the Table name and the unresolved column list and add these to a Map, Key: Table name and Value: Unresolved column list
//    if (unresolvedColumnList.length < 3) { // When the query only retrieves data from one table.
//      // Extract the table name
//      tableName = unresolvedColumnList(0).toString
//      attribList += tableName.toLowerCase() + ";from"
//      attribExtendedList += tableName.toLowerCase() + ";from"
//
//      // Add the table name as key and the unresolved column list as value to the Map tableColumnMap.
//      tableColumnMap += (tableName -> unresolvedColumnList(1))
//
////      println("Table Name: " + qryAliases(0).toString)
////      println("Column Names: " + qryAliases(1))
//    } else { // When the query contains join statements.
//      unresolvedColumnList.foreach {
//        i =>
//          /* Starting from position 1 every third position is the table name and every third position from starting
//             position 2 is the unresolved column list which should be added as value to the Map tableColumnMap. */
//          if (unresolvedColumnList.indexOf(i) < 3 && (unresolvedColumnList.indexOf(i) % 3) == 1) {
//            tableName = i.toString
//            attribList += tableName.toLowerCase() + ";from"
//            attribExtendedList += tableName.toLowerCase() + ";from"
//          } else if (unresolvedColumnList.indexOf(i) > 3 && (unresolvedColumnList.indexOf(i) % 3) == 1) {
////            println("Table Name: " + i)
//            tableName = i.toString
//          } else if ((unresolvedColumnList.indexOf(i) > 0 && unresolvedColumnList.indexOf(i) < 3) && ((unresolvedColumnList.indexOf(i) % 3) % 2) == 0) {
////            println("Column Names: " + i.getClass)
//
//            // IF-statement when self-joining.
//            if (!tableColumnMap.contains(tableName)) {
//              tableColumnMap += (tableName -> i)
//            }
////            tableColumnMap += (tableName -> i)
//          } else if (unresolvedColumnList.indexOf(i) > 3 && (unresolvedColumnList.indexOf(i) % 3) == 2) {
////            println("Column Names: " + i.getClass)
//
//            // IF-statement when self-joining.
//            if (!tableColumnMap.contains(tableName)) {
//              tableColumnMap += (tableName -> i)
//            }
////            tableColumnMap += (tableName -> i)
//          }
//      }
  }


  /**
   * Get the complete table complementary to the attribute name using the unresolved table name.
   * @param unresolvedColumnList - List with attributes containing the unresolved table name.
   */
//  def getTableAttribName(unresolvedColumnList: Seq[Object]) {
  def getTableAttribName(unresolvedColumnList: Seq[Object]): List[String] = {
    var tableName: String = ""
    var tableColumnMap = Map[String, Object]()
    val attribList: ListBuffer[String] = ListBuffer()
    val attribExtendedList: ListBuffer[String] = ListBuffer()
    var isFromTable: Boolean = true

    // Extract the Table name and the unresolved column list and add these to a Map, Key: Table name and Value: Unresolved column list
//    if (unresolvedColumnList.length < 3) { // When the query only retrieves data from one table.
//      // Extract the table name
//      tableName = unresolvedColumnList(0).toString
//      attribList += tableName.toLowerCase() + ";from"
//      attribExtendedList += tableName.toLowerCase() + ";from"
//
//      // Add the table name as key and the unresolved column list as value to the Map tableColumnMap.
//      tableColumnMap += (tableName -> unresolvedColumnList(1))
//
//      //      println("Table Name: " + qryAliases(0).toString)
//      //      println("Column Names: " + qryAliases(1))
//    } else { // When the query contains join statements.
//      unresolvedColumnList.foreach {
//        i =>
//          /* Starting from position 1 every third position is the table name and every third position from starting
//             position 2 is the unresolved column list which should be added as value to the Map tableColumnMap. */
//          if (unresolvedColumnList.indexOf(i) < 3 && (unresolvedColumnList.indexOf(i) % 3) == 1) {
//            tableName = i.toString
//            attribList += tableName.toLowerCase() + ";from"
//            attribExtendedList += tableName.toLowerCase() + ";from"
//          } else if (unresolvedColumnList.indexOf(i) > 3 && (unresolvedColumnList.indexOf(i) % 3) == 1) {
////            println("Table Name: " + i)
//            tableName = i.toString
//          } else if ((unresolvedColumnList.indexOf(i) > 0 && unresolvedColumnList.indexOf(i) < 3) && ((unresolvedColumnList.indexOf(i) % 3) % 2) == 0) {
////            println("Column Names: " + i.getClass)
//
//            // IF-statement when self-joining.
//            if (!tableColumnMap.contains(tableName)) {
//              tableColumnMap += (tableName -> i)
//            }
////            tableColumnMap += (tableName -> i)
//          } else if (unresolvedColumnList.indexOf(i) > 3 && (unresolvedColumnList.indexOf(i) % 3) == 2) {
////            println("Column Names: " + i.getClass)
//
//            // IF-statement when self-joining.
//            if (!tableColumnMap.contains(tableName)) {
//              tableColumnMap += (tableName -> i)
//            }
////            tableColumnMap += (tableName -> i)
//          }
//      }
//    }

    for (i <- 0 until unresolvedColumnList.length) {
      if (unresolvedColumnList(i).toString.contains("List")) {
        tableName = unresolvedColumnList(i - 1).toString.toLowerCase()

        if (isFromTable) {
          attribList += tableName + ";from"
          attribExtendedList += tableName + ";from"
//          println("From: " + tableName)
          isFromTable = false
        }

        // IF-statement when self-joining.
        if (!tableColumnMap.contains(tableName)) {
          tableColumnMap += (tableName -> unresolvedColumnList(i).toString.toLowerCase())
        }
        //        println("The unresolved column list:")
        //        println(unresolvedColumnList(i))
        //        println("The previous value is: " + unresolvedColumnList(i - 1))
      }
    }

    // Testing, see(print) the content of the map.
//    println("Map content:")
//    tableColumnMap foreach {
//      case (key, value) => println (key + " --> " + value)
//    }

    attributeSet.foreach {
      i =>
//        println(i)
        // Create new regex based on unresolved column name.
        val splittedString: Array[String] = i.split(";")
        val attribRegx: Regex = new Regex(splittedString(0))
        val aggregateCntSlctAllRegx: Regex = ".+\\;(select|count\\(\\*\\))".r

        // Add the specific aggregate clause of select * or count(*) and add to the QCS Extended List.
        aggregateCntSlctAllRegx.findAllIn(i.toString()).matchData foreach {
          m =>
//            println("Pickle Rick: " + m.group(0))
            attribList += m.group(0)
        }

        // Iterate through map and find matching unresolved column name of table.
        for ((table, columnList) <- tableColumnMap) {
          if (!attribRegx.findFirstIn(columnList.toString.toLowerCase()).isEmpty) {
            val completeAttribName: String = table + "." + i.split("#")(0)
//            println(completeAttribName)
            attribList += completeAttribName + ";" + splittedString(1)
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

//    println("Original: " + joinClauseList)
//    println("Reverse: " + joinClauseList.reverse)
//    println("-----------------------------------------------------------------------------------------------------------------\n")

    // Iterate over joinClauseList to get the correct order of joining statements.
    joinClauseList.reverse.foreach {
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

        if ((!joinColumnName2.isEmpty && !joinColumnName1.isEmpty)) {
          val joinClause: String = joinColumnName1 + " = " + joinColumnName2
//          println(joinClause + ";join")
          attribList += joinClause + ";join"
          attribExtendedList += joinClause + ";join"
//          println("String1 or String2 is empty")
        }

//        println("joinColumnName1 is: " + joinColumnName1 + ", Boolean: " + !joinColumnName1.isEmpty)
//        println("joinColumnName2 is: " + joinColumnName2 + ", Boolean: " + !joinColumnName2.isEmpty)

//        val joinClause: String = joinColumnName1 + " = " + joinColumnName2

//        println(joinClause + ";join")
//        println(List(joinColumnName1 + " = " + joinColumnName2))
//        attribList += joinClause + ";join"
//        attribExtendedList += joinClause + ";join"
    }

//    println(attribList.toList)
//    println(attribExtendedList.toList)

    qcsExtended += attribExtendedList.toList
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
    val outputCsv = new File("/Users/eddy/Documents/study_github/2IMC00_thesis_benchmark/output_qcs.csv")

    val writer = CSVWriter.open(outputCsv)

    totalQCSMap foreach {
      case (key, value) =>
        writer.writeRow(List(value, key.sortBy(_.length).mkString(", ")))
    }

    writer.close()
  }


  /**
   * Generate/create queries based on the attributes using their labelling.
   * @param QCSMap QCSMap containing the qcs entries which is used to generate queries.
   */
  def generateQueries(QCSExtendedMap: ListBuffer[List[String]]): Unit = {
    val generatedQueriesList: ListBuffer[List[String]] = ListBuffer()
    val queriesCsv = new File("/Users/eddy/Documents/study_github/2IMC00_thesis_benchmark/output_qcs_queries.csv")
    val writer = CSVWriter.open(queriesCsv)

    // Different Regular Expressions, to specifically create a part of a query.
    val selectRegxr: Regex = "(\\w+.?\\w+)\\;(select|avg|sum|count\\(\\*\\)|count)".r
    val fromRegxr: Regex = "(\\w+)\\;(from)".r
    val joiningRegxr: Regex = "(\\w+\\.\\w+\\s\\=\\s\\w+\\.\\w+)\\;(join)".r
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

//            println(qryBase(i))

          // Create the 'SELECT' statement based on the Regex selectRegxr.
//          selectRegxr.findAllIn(qryBase(i)).matchData foreach {
//            m =>
//              if (m.group(2) == "avg" | m.group(2) == "sum" | m.group(2) == "count") {
////                println("Pickle Rick: " + m.group(2) + "(" + m.group(1) + ")")
//                selectPart += m.group(2) + "(" + m.group(1) + "), "
//              }
//
//              if (m.group(2) == "count(*)" && m.group(1) == "ALL") {
////                  println("Pickle Rick: " + m.group(2))
//                selectPart += m.group(2) + ", "
//              }
//
//              if (m.group(2) == "select") {
////                println("Pickle Rick: " + " * ")
//                selectPart += "*   "
//                groupByPart = ""
//              }
//
//          }

          // Create the 'JOIN' statements based on the Regex joiningRegxr.
          joiningRegxr.findAllIn(qryBase(i)).matchData foreach {
            m =>
              val tmpTable1: String = m.group(1).split("\\.")(0).toLowerCase()
              val tmpTable2: String = m.group(1).split(" = ")(1).split("\\.")(0)
              var joiningTable: String = ""

//              println("FROM: " + fromPart.split("\\s")(1).toLowerCase())
//              println("Regex: " + m)
//              println("First Table: " + tmpTable1)
//              println("Second Table: " + tmpTable2)
//              println("Same as FROM: " + fromPart.split("\\s")(1).equalsIgnoreCase(tmpTable1))
//              println("Already in join: " + joiningPart.contains(tmpTable1 + "."))

              // TESTING INSTEAD OF COMPARING USE CONTAINS().
              if (fromPart.split("\\s")(1).toLowerCase() == tmpTable1.toLowerCase() || joiningPart.contains(tmpTable1 + ".")) {
//                println("JOININGPART CONTAINS: " + tmpTable1)
//                println("WITHOUT FIX: " + m.group(2) + " " + tmpTable1 + " on " + m.group(1))
//                println("SHOULD BE: " + m.group(2) + " " + tmpTable2 + " on " + m.group(1))
                joiningTable = tmpTable2
              } else {
                joiningTable = tmpTable1
              }

              joiningPart += m.group(2) + " " + joiningTable + " on " + m.group(1) + " "
//              joiningPart += m.group(2) + " " + tmpTable1 + " on " + m.group(1) + " "

              // FOR TESTING: BY PRINTING THE JOIN STATEMENT.
//              println(joiningPart)
//              println("-----------------------------------------------------------------------------------------------")
          }

          // Create the 'WHERE' statement based on the Regex whereRegxr.
//          whereRegxr.findAllIn(qryBase(i)).matchData foreach {
//            m =>
////                wherePart += m.group(1) + " " + m.group(2) + " and "
////              println("Join: " + joiningPart)
////              println(m.group(1).split("\\.")(0))
//              if (joiningPart.contains(m.group(1).split("\\.")(0)) || fromPart.contains(m.group(1).split("\\.")(0))) {
//                wherePart += m.group(1) + " " + m.group(2) + " " + m.group(3) + " and "
//              }
//
////              wherePart += m.group(1) + " " + m.group(2) + " " + m.group(3) + " and "
//
//          }

          // Create the 'GROUP BY' statements based on the Regex groupByRegxr.
//          groupByRegxr.findAllIn(qryBase(i)).matchData foreach {
//            m =>
//              println("join: " + joiningPart)
//              println("table name: " + m.group(1).split("\\.")(0))
//              groupByPart += m.group(1)  + ", "
//          }
        }

        // Loop through every QCS List.
        for (i <- 0 until qryBase.length) {
          // Create the 'GROUP BY' statements based on the Regex groupByRegxr.
          groupByRegxr.findAllIn(qryBase(i)).matchData foreach {
            m =>
              if (fromPart.contains(m.group(1).split("\\.")(0)) || joiningPart.contains(m.group(1).split("\\.")(0) + ".")) {
                groupByPart += m.group(1)  + ", "
              }

//              groupByPart += m.group(1)  + ", "
          }
        }

        // Loop through every QCS List a second time such that certain query parts (From, Join, Group By) have already been defined.
        for (i <- 0 until qryBase.length) {
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
                groupByPart = ""
              }

          }

          // Create the 'WHERE' statement based on the Regex whereRegxr.
          whereRegxr.findAllIn(qryBase(i)).matchData foreach {
            m =>
//                wherePart += m.group(1) + " " + m.group(2) + " and "

              // Only add the where table if the tables is mentioned in the From or Join statement.
              if (fromPart.contains(m.group(1).split("\\.")(0)) || joiningPart.contains(m.group(1).split("\\.")(0))) {
                wherePart += m.group(1) + " " + m.group(2) + " " + m.group(3) + " and "
              }

//              wherePart += m.group(1) + " " + m.group(2) + " " + m.group(3) + " and "
          }
        }

        // Removing the complete where statement, when the 'table.attributeName' is not stated in the from or join statement.
        if (wherePart.equalsIgnoreCase("where ")) {
          wherePart = ""
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
   * Create the qcs based on the queries and using different methods to extract the specific attributes for the QCS.
   * @param queries List with queries from which a QCS has to be generated.
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
//      println(qryAliases.foreach(entry => println(entry)))
//
//      println("###################################################################################################################")

      getAggregateAttrib(aggregateClause)
      getWhereAttrib(whereClause)
      getGroupbyAttrib(groupByClause)
      getJoiningClause(joinClause)

//      testQueryAlias(qryAliases)
      qcsResults  += getTableAttribName(qryAliases)

//      println(attributeSet)
//      println(joinClauseList)
//      println(qcsResults)
//
//      println("-------------------------------------------------------------------------------------------------------------------")

      // Clear the Sets for new iteration.
      attributeSet = attributeSet.empty
      attributeExtendedSet = attributeExtendedSet.empty
//      joinClauseList = joinClauseList.empty
      joinClauseList.clear()
    }
//    println(qcsResults)

    val qcsMap: Map[List[String], Int] = qcsFrequency(qcsResults)

//    qcsMap.foreach(println)

    qcsToCsv(qcsMap)
    generateQueries(qcsExtended)

  }

  def getQueriesCsv(outputName: String): Unit = {
//    val outputDir: String = "/Users/eddy/Documents/study_github/2IMC00_thesis_benchmark/correlation_tables/qcs_attributes_output/"
    val outputDir: String = "/Users/eddy/Documents/study_github/2IMC00_thesis_benchmark/correlation/"

    val qcsTableColumn: String = "SELECT distinct g.clean as galaxy_clean, g.dec as galaxy_dec, g.g as galaxy_g, g.petromag_r as galaxy_petromag_r, g.petromag_u as galaxy_petromag_u, g.petror90_g as galaxy_petror90_g, g.petror90_r as galaxy_petror90_r, g.petrorad_u as galaxy_petrorad_u, g.r as galaxy_r, g.ra as galaxy_ra, gt.dec as galaxytag_dec, gt.ra as galaxytag_ra, gt.type as galaxytag_type, gse.bptclass as galspecextra_bptclass, gse.sfr_fib_p50 as galspecextra_sfr_fib_p50, gse.sfr_tot_p50 as galspecextra_sfr_tot_p50, gse.sfr_tot_p84 as galspecextra_sfr_tot_p84, gse.specsfr_tot_p50 as galspecextra_specsfr_tot_p50, gsi.d4000_n as galspecindx_d4000_n, gs.h_alpha_eqw as galspecline_h_alpha_eqw, gs.h_alpha_flux as galspecline_h_alpha_flux, gs.h_alpha_flux_err as galspecline_h_alpha_flux_err, gs.h_beta_eqw as galspecline_h_beta_eqw, gs.h_beta_flux as galspecline_h_beta_flux, gs.h_beta_flux_err as galspecline_h_beta_flux_err, gs.nii_6584_flux as galspecline_nii_6584_flux, gs.oi_6300_flux_err as galspecline_oi_6300_flux_err, gs.oiii_5007_eqw as galspecline_oiii_5007_eqw, gs.oiii_5007_flux as galspecline_oiii_5007_flux, gs.sii_6717_flux as galspecline_sii_6717_flux, gs.sii_6731_flux_err as galspecline_sii_6731_flux_err, po.b as photoobj_b, po.camcol as photoobj_camcol, po.clean as photoobj_clean, po.cmodelmag_g as photoobj_cmodelmag_g, po.dec as photoobj_dec, po.devrad_g as photoobj_devrad_g, po.devrad_r as photoobj_devrad_r, po.fibermag_r as photoobj_fibermag_r, po.field as photoobj_field, po.flags as photoobj_flags, po.fracdev_r as photoobj_fracdev_r, po.g as photoobj_g, po.l as photoobj_l, po.mode as photoobj_mode, po.petromag_r as photoobj_petromag_r, po.petromag_z as photoobj_petromag_z, po.petror50_g as photoobj_petror50_g, po.petror50_r as photoobj_petror50_r, po.petrorad_g as photoobj_petrorad_g, po.petrorad_r as photoobj_petrorad_r, po.r as photoobj_r, po.ra as photoobj_ra, po.run as photoobj_run, po.type as photoobj_type, po.u as photoobj_u, poa.camcol as photoobjall_camcol, poa.clean as photoobjall_clean, poa.dec as photoobjall_dec, poa.dered_r as photoobjall_dered_r, poa.devrad_r as photoobjall_devrad_r, poa.devraderr_r as photoobjall_devraderr_r, poa.exprad_r as photoobjall_exprad_r, poa.field as photoobjall_field, poa.fracdev_r as photoobjall_fracdev_r, poa.mode as photoobjall_mode, poa.petromag_r as photoobjall_petromag_r, poa.ra as photoobjall_ra, poa.run as photoobjall_run, poa.type as photoobjall_type, poa.u as photoobjall_u, pt.clean as phototag_clean, pt.dec as phototag_dec, pt.mode as phototag_mode, pt.nchild as phototag_nchild, pt.psfmag_r as phototag_psfmag_r, pt.ra as phototag_ra, pt.type as phototag_type, pz.absmagr as photoz_absmagr, pz.photoerrorclass as photoz_photoerrorclass, pz.nncount as photoz_nncount, pz.nnvol as photoz_nnvol, pz.z as photoz_z, pz.zerr as photoz_zerr, sp.dec as specphoto_dec, sp.mode as specphoto_mode, sp.modelmag_r as specphoto_modelmag_r, sp.petromag_r as specphoto_petromag_r, sp.petromag_z as specphoto_petromag_z, sp.ra as specphoto_ra, sp.type as specphoto_type, sp.z as specphoto_z, sp.zwarning as specphoto_zwarning, sps.fehadop as sppparams_fehadop, smfged.logmass as stellarmassfspsgranearlydust_logmass, smfged.z as stellarmassfspsgranearlydust_z, zs.elliptical as zoospec_elliptical, zs.p_cs as zoospec_p_cs, zs.p_cs_debiased as zoospec_p_cs_debiased, zs.p_el as zoospec_p_el, zs.p_el_debiased as zoospec_p_el_debiased, zs.spiral as zoospec_spiral, zs.uncertain as zoospec_uncertai FROM photoobjall poa JOIN photoobj po on po.objid = poa.objid JOIN galaxy g on g.objid = poa.objid JOIN galaxytag gt on gt.objid = poa.objid JOIN galspecextra gse on gse.specobjid = poa.specobjid JOIN galspecindx gsi on gsi.specobjid = poa.specobjid JOIN galspecline gs on gs.specobjid = poa.specobjid JOIN phototag pt on pt.objid = poa.objid JOIN photoz pz on pz.objid = poa.objid JOIN specphoto sp on sp.objid = poa.objid JOIN sppparams sps on sps.bestobjid = poa.objid JOIN stellarmassfspsgranearlydust smfged on smfged.specobjid = poa.specobjid JOIN zoospec zs on zs.objid = poa.objid"

    spark.sql(qcsTableColumn).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").format("csv").save(outputDir + outputName)

//    qryResult.show()
  }

}
