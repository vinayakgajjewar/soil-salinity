package edu.ucr.cs.bdlab.raptor

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, WebMethod}
import edu.ucr.cs.bdlab.beast.indexing.RTreeFeatureReader
import edu.ucr.cs.bdlab.beast.io.{GeoJSONFeatureReader, SpatialFileRDD}
import edu.ucr.cs.bdlab.beast.util.AbstractWebHandler
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory}
import org.locationtech.jts.io.ParseException

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer

class NDVIServlet extends AbstractWebHandler with Logging {

  /** Additional options passed on by the user to override existing options */
  var opts: BeastOptions = _

  /** The SparkSession that is used to process datasets */
  var sparkSession: SparkSession = _

  /** The path at which this server keeps all datasets */
  var ndviDataPath: Path = _

  override def setup(ss: SparkSession, opts: BeastOptions): Unit = {
    super.setup(ss, opts)
    this.opts = opts
    this.sparkSession = ss
    val dataPath: String = opts.getString("datapath", "data")
    ndviDataPath = new Path(dataPath, "NDVI")

    // Build indexes if not existent
    logInfo("Building raster indexes for NDVI")
    val sc = ss.sparkContext
    val fs = ndviDataPath.getFileSystem(sc.hadoopConfiguration)
    val directories = fs.listStatus(ndviDataPath,
      (path: Path) => path.getName.matches("\\d+-\\d+-\\d+"))
    for (dir <- directories) {
      val indexPath = new Path(dir.getPath, "_index.csv")
      if (!fs.exists(indexPath)) {
        logInfo(s"Building a raster index for '${dir.getPath}'")
        RasterFileRDD.buildIndex(sc, dir.getPath.toString, indexPath.toString)
      }
    }
  }

  /**
   * Computes the NDVI time series for a give query polygon and a time range
   * @param path
   * @param request
   * @param response
   * @return
   */
  @WebMethod(url = "/ndvi/singlepolygon.json", order = 1)
  def singlePolygon(path: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    // Date range
    var dateFrom = ""
    var dateTo = ""

    // try getting parameters from url
    try {
      dateFrom = request.getParameter("from")
      dateTo = request.getParameter("to")
    } catch {
      case e: NullPointerException => throw new RuntimeException("Couldn't find the required parameters: from and to")
    }

    // set content-type as application/json// set content-type as application/json
    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)

    // load raster data based on selected date range
    val fileSystem = ndviDataPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val matchingRasterDirs: Array[String] = fileSystem.listStatus(ndviDataPath,
      (path: Path) => NDVIServlet.dateRangeOverlap(dateFrom, dateTo, path.getName))
      .map(_.getPath.toString)

    val baos = new ByteArrayOutputStream
    val input = request.getInputStream
    IOUtils.copy(input, baos)
    input.close()
    baos.close()
    val geoJSONData: Array[Byte] = baos.toByteArray
    var geom: Geometry = null
    try {
      val jsonParser = new JsonFactory().createParser(new ByteArrayInputStream(geoJSONData))
      geom = GeoJSONFeatureReader.parseGeometry(jsonParser)
      geom.setSRID(4326)
      jsonParser.close()
    } catch {
      case e: Exception =>
        logError(s"Error parsing GeoJSON geometry ${new String(geoJSONData)}", e)
        throw new RuntimeException(s"Error parsing query geometry ${new String(geoJSONData)}", e)
    }

    // Now that we have the query geometry, loop over al matching directories and run the NDVI query
    val results: Array[(String, Float)] = matchingRasterDirs.map(matchingRasterDir => {
      val matchingFiles = RasterFileRDD.selectFiles(fileSystem, matchingRasterDir, geom)
      if (matchingFiles.isEmpty)
        null
      else {
        // Use RaptorJoin to find the results
        val results = SingleMachineRaptorJoin.raptorJoin[Array[Int]](matchingFiles, Array(geom))
        // Since we have one geometry, all the results are for a single geometry
        val mean = results
          .filter(x => x._2(0) + x._2(3) > 0) // Drop records with a denominator of zero
          .map(x => (x._2(0).toFloat - x._2(3)) / (x._2(0).toFloat + x._2(3))) // NDVI Equation
          .aggregate((0.0F,0))((sumCount, v) => (sumCount._1 + v, sumCount._2 + 1),
          (sumCount1, sumCount2) => (sumCount1._1 + sumCount2._1, sumCount1._2 + sumCount2._2))
        (new Path(matchingRasterDir).getName, mean._1 / mean._2)
      }
    }).filter(_ != null)

    // write result to json object
    val resWriter = response.getWriter
    val mapper = new ObjectMapper

    // create query node
    val queryNode = mapper.createObjectNode
    queryNode.put("from", dateFrom)
    queryNode.put("to", dateTo)

    // create results node
    val resultsNode = mapper.createArrayNode()
    for (result <- results) {
      val resultNode = mapper.createObjectNode()
      resultNode.put("date", result._1)
      resultNode.put("mean", result._2)
      resultsNode.add(resultNode)
    }

    // Create a root node that contains queryNode and resultsNode
    val rootNode = mapper.createObjectNode
    rootNode.set("query", queryNode)
    rootNode.set("results", resultsNode)

    // write values to response writer
    val jsonString = mapper.writer.writeValueAsString(rootNode)
    resWriter.print(jsonString)
    resWriter.flush()
    true
  }

  @WebMethod(url = "/ndvi/{datasetID}.json", order = 99)
  def queryVector(path: String, request: HttpServletRequest, response: HttpServletResponse, datasetID: String): Boolean = {
    // time at start of GET request
    val t1 = System.nanoTime

    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)

    // Load the Farmland features// Load the Farmland features
    val indexPath = new Path(VectorServlet.getIndexPathById(VectorServlet.VectorIndexFile, datasetID))
    val reader = new RTreeFeatureReader
    val fs = indexPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val fileLength = fs.getFileStatus(indexPath).getLen
    val inputFileSplit = new FileSplit(indexPath, 0, fileLength, null)
    val opts = new BeastOptions
    var dateFrom: String = null
    var dateTo: String = null
    var mbr: Envelope = null
    var searchGeom: Geometry = null
    try {
      // get sidebar select parameters
      dateFrom = request.getParameter("from")
      dateTo = request.getParameter("to")
      // get extents parameters
      val minx = request.getParameter("minx").toDouble
      val miny = request.getParameter("miny").toDouble
      val maxx = request.getParameter("maxx").toDouble
      val maxy = request.getParameter("maxy").toDouble
      mbr = new Envelope(minx, maxx, miny, maxy)
      searchGeom = new GeometryFactory().toGeometry(mbr)
      searchGeom.setSRID(4326)
      opts.set(SpatialFileRDD.FilterMBR, Array(minx, miny, maxx, maxy).mkString(","))
    } catch {
      case e: NullPointerException =>
    }
    // MBR not passed. Use all farmlands
    if (dateFrom == null || dateTo == null) {
      val writer = response.getWriter
      writer.printf("{\"error\": \"Error! Both 'from' and 'to' parameters are required\"}")
      return true
    }
    // Initialize the reader that reads the relevant farmlands
    reader.initialize(inputFileSplit, opts)
    // Retrieve in an array to prepare the zonal statistics calculation// Retrieve in an array to prepare the zonal statistics calculation
    val farmlands = reader.asScala.toArray
    reader.close()
    logInfo(s"Read ${farmlands.length} records in ${(System.nanoTime() - t1) *1E-9} seconds")

    // load raster data based on selected date range
    val fileSystem = ndviDataPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val matchingRasterDirs: Array[String] = fileSystem.listStatus(ndviDataPath,
        (path: Path) => NDVIServlet.dateRangeOverlap(dateFrom, dateTo, path.getName))
      .map(_.getPath.toString)

    val finalResults: Array[ArrayBuffer[(String, Float)]] = new Array[ArrayBuffer[(String, Float)]](farmlands.length)
      .map(_ => new ArrayBuffer[(String, Float)]())
    val geoms: Array[Geometry] = farmlands.map(_.getGeometry)
    for (matchingRasterDir <- matchingRasterDirs) {
      val date = new Path(matchingRasterDir).getName
      val matchingFiles = RasterFileRDD.selectFiles(fileSystem, matchingRasterDir, searchGeom)
      if (matchingFiles.nonEmpty) {
        // Use RaptorJoin to find the results
        val rjResults = SingleMachineRaptorJoin.raptorJoin[Array[Int]](matchingFiles, geoms)
        // Since we have one geometry, all the results are for a single geometry
        val averages: Array[(Float, Int)] = Array.fill(geoms.length)((0.0f, 0))
        val ndvis: Iterator[(Long, Float)] = rjResults
          .filter(x => x._2(0) + x._2(3) > 0) // Drop records with a denominator of zero
          .map(x => (x._1, (x._2(0).toFloat - x._2(3)) / (x._2(0).toFloat + x._2(3)))) // NDVI Equation
        // Note that the RaptorJoin results are ordered by geometry ID
        for (ndvi <- ndvis) {
          val sumCount: (Float, Int) = averages(ndvi._1.toInt)
          averages(ndvi._1.toInt) = (sumCount._1 + ndvi._2, sumCount._2 + 1)
        }
        for (iGeom <- averages.indices)
          finalResults(iGeom).append((date, averages(iGeom)._1 / averages(iGeom)._2))
      }
    }

    // write results to json object// write results to json object
    val out = response.getWriter
    val mapper = new ObjectMapper

    // create query node// create query node
    val queryNode = mapper.createObjectNode
    queryNode.put("from", dateFrom)
    queryNode.put("to", dateTo)

    // create mbr node// create mbr node
    // inside query node// inside query node
    if (mbr != null) {
      val mbrNode = mapper.createObjectNode
      mbrNode.put("minx", mbr.getMinX)
      mbrNode.put("miny", mbr.getMinY)
      mbrNode.put("maxx", mbr.getMaxX)
      mbrNode.put("maxy", mbr.getMaxY)
      queryNode.set("mbr", mbrNode)
    }

    // create results node
    val resultsNode = mapper.createArrayNode

    // populate json object with max vals
    for (i <- finalResults.indices) {
      val s: ArrayBuffer[(String, Float)] = finalResults(i)
      if (s != null) {
        val resultNode = mapper.createObjectNode
        resultNode.put("objectid", farmlands(i).getAs("OBJECTID").asInstanceOf[Number].longValue)
        val ndvis = mapper.createArrayNode()
        for ((date, ndvi) <- s) {
          val ndviNode = mapper.createObjectNode()
          ndviNode.put("date", date)
          ndviNode.put("mean", ndvi)
          ndvis.add(ndviNode)
        }
        resultNode.set("results", ndvis)
        resultsNode.add(resultNode)
      }
    }

    // create root node// create root node
    // contains queryNode and resultsNode// contains queryNode and resultsNode
    val rootNode = mapper.createObjectNode
    rootNode.set("query", queryNode)
    rootNode.set("results", resultsNode)

    // write values to response writer// write values to response writer
    val jsonString = mapper.writer.writeValueAsString(rootNode)
    out.print(jsonString)
    out.flush()
    true
  }
}

object NDVIServlet {
  def dateRangeOverlap(from: String, to: String, date: String): Boolean = {
    val fromParts = from.split("-").map(_.toInt)
    val toParts = to.split("-").map(_.toInt)
    val dateParts = date.split("-").map(_.toInt)
    fromParts.indices.forall(i => dateParts(i) >= fromParts(i) && dateParts(i) <= toParts(i))
  }
}