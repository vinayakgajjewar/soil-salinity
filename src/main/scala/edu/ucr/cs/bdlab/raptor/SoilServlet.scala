package edu.ucr.cs.bdlab.raptor

import com.fasterxml.jackson.databind.ObjectMapper
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, WebMethod}
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.indexing.RTreeFeatureReader
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD
import edu.ucr.cs.bdlab.beast.util.AbstractWebHandler
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory}
import org.locationtech.jts.io.ParseException
import org.locationtech.jts.io.geojson.GeoJsonReader

import java.io.ByteArrayOutputStream
import java.util
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.collection.JavaConverters.asScalaIteratorConverter

class SoilServlet extends AbstractWebHandler with Logging {

  /** Additional options passed on by the user to override existing options */
  var opts: BeastOptions = _

  /** The SparkSession that is used to process datasets */
  var sparkSession: SparkSession = _

  /** The path at which this server keeps all datasets */
  var dataPath: String = _

  override def setup(ss: SparkSession, opts: BeastOptions): Unit = {
    super.setup(ss, opts)
    this.opts = opts
    this.sparkSession = ss
    this.dataPath = opts.getString("datapath", "data")
  }

  @WebMethod(url = "/soil/singlepolygon.json")
  def singlePolygon(path: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    // sidebar select parameters// sidebar select parameters
    var soilDepth = ""
    var layer = ""

    // try getting parameters from url// try getting parameters from url
    try {
      soilDepth = request.getParameter("soildepth")
      layer = request.getParameter("layer")
    } catch {
      case e: NullPointerException => throw new RuntimeException("Couldn't find the required parameters: soildepth and layer")
    }

    // set content-type as application/json// set content-type as application/json
    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)

    // set Access-Control-Allow-Origin// set Access-Control-Allow-Origin
    // otherwise, the front-end won't be able to make GET requests to this server// otherwise, the front-end won't be able to make GET requests to this server
    // because of CORS policy// because of CORS policy
    response.addHeader("Access-Control-Allow-Origin", "*")

    // load raster data based on selected soil depth and layer// load raster data based on selected soil depth and layer
    val matchingRasterFiles: Array[String] = SoilServlet2.rasterFiles
      .filter(rasterFile => SoilServlet.rangeOverlap(rasterFile._1, soilDepth))
      .map(rasterFile => s"data/tif/${rasterFile._2}/$layer.tif")
      .toArray

    val baos = new ByteArrayOutputStream
    val input = request.getInputStream
    IOUtils.copy(input, baos)
    input.close()
    baos.close()

    // initialize geojson reader to parse the query geometry
    val reader = new GeoJsonReader(new GeometryFactory)

    // try reading the geojson string into a geometry object// try reading the geojson string into a geometry object
    var geom: Geometry = null
    val geometryGeoJSON = baos.toString
    try {
      geom = reader.read(geometryGeoJSON)
      geom.setSRID(4326)
    } catch {
      case e: ParseException =>
        System.err.println("----ERROR: could not parse geojson string " + geometryGeoJSON)
        e.printStackTrace()
    }

    val geomArray = Array(geom)

    // now that we have a geometry object// now that we have a geometry object
    // call single machine raptor join// call single machine raptor join
    val singleMachineResults: SingleMachineRaptorJoin.Statistics = SingleMachineRaptorJoin.join(matchingRasterFiles, geomArray)

    // write result to json object// write result to json object
    val resWriter = response.getWriter
    val mapper = new ObjectMapper

    // create query node// create query node
    val queryNode = mapper.createObjectNode
    queryNode.put("soildepth", soilDepth)
    queryNode.put("layer", layer)

    // create results node// create results node
    val resultsNode = mapper.createObjectNode
    if (singleMachineResults != null) {
      resultsNode.put("max", singleMachineResults.max)
      resultsNode.put("min", singleMachineResults.min)
      resultsNode.put("median", singleMachineResults.median)
      resultsNode.put("sum", singleMachineResults.sum)
      resultsNode.put("mode", singleMachineResults.mode)
      resultsNode.put("stddev", singleMachineResults.stdev)
      resultsNode.put("count", singleMachineResults.count)
      resultsNode.put("mean", singleMachineResults.mean)
    }

    // create root node// create root node
    // contains queryNode and resultsNode// contains queryNode and resultsNode
    val rootNode = mapper.createObjectNode
    rootNode.set("query", queryNode)
    rootNode.set("results", resultsNode)

    // write values to response writer// write values to response writer
    val jsonString = mapper.writer.writeValueAsString(rootNode)
    resWriter.print(jsonString)
    resWriter.flush()
    true
  }

  @WebMethod(url = "/soil/{datasetID}.json")
  def queryVector(path: String, request: HttpServletRequest, response: HttpServletResponse, datasetID: String): Boolean = {
    // time at start of GET request
    val t1 = System.nanoTime

    // we set content-type as application/geo+json// we set content-type as application/geo+json
    // not application/json// not application/json
    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)

    // set Access-Control-Allow-Origin// set Access-Control-Allow-Origin
    // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy// otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
    response.addHeader("Access-Control-Allow-Origin", "*")

    // Load the Farmland features// Load the Farmland features
    val indexPath = new Path(VectorServlet.getIndexPathById(VectorServlet.VectorIndexFile, datasetID))
    val reader = new RTreeFeatureReader
    val fs = indexPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val fileLength = fs.getFileStatus(indexPath).getLen
    val inputFileSplit = new FileSplit(indexPath, 0, fileLength, null)
    val opts = new BeastOptions
    var soilDepth: String = null
    var layer: String = null
    var mbr: Envelope = null
    try {
      // get sidebar select parameters
      soilDepth = request.getParameter("soildepth")
      layer = request.getParameter("layer")
      // get extents parameters
      val minx = request.getParameter("minx").toDouble
      val miny = request.getParameter("miny").toDouble
      val maxx = request.getParameter("maxx").toDouble
      val maxy = request.getParameter("maxy").toDouble
      mbr = new Envelope(minx, maxx, miny, maxy)
      opts.set(SpatialFileRDD.FilterMBR, Array(minx, miny, maxx, maxy).mkString(","))
    } catch {
      case e: NullPointerException =>
    }
    // MBR not passed. Use all farmlands
    if (soilDepth == null || layer == null) {
      val writer = response.getWriter
      writer.printf("{\"error\": \"Error! Both 'soildepth' and 'layer' parameters are required\"}")
      return true
    }
    // Initialize the reader that reads the relevant farmlands// Initialize the reader that reads the relevant farmlands
    reader.initialize(inputFileSplit, opts)

    // Retrieve in an array to prepare the zonal statistics calculation// Retrieve in an array to prepare the zonal statistics calculation
    val farmlands = new util.ArrayList[IFeature]
    for (farmland <- reader.asScala) {
      farmlands.add(farmland)
    }
    reader.close()
    logInfo(s"Read ${farmlands.size()} records in ${(System.nanoTime() - t1) *1E-9} seconds")

    // load raster data based on selected soil depth and layer// load raster data based on selected soil depth and layer
    val matchingRasterFiles: Array[String] = SoilServlet2.rasterFiles
      .filter(rasterFile => SoilServlet.rangeOverlap(rasterFile._1, soilDepth))
      .map(rasterFile => s"data/tif/${rasterFile._2}/$layer.tif")
      .toArray

    // Load raster data// Load raster data
    var finalResults: Array[Collector] = null
    val rasterReader = new GeoTiffReader[Float]
    for (matchingRasterFile <- matchingRasterFiles) {
      rasterReader.initialize(fs, matchingRasterFile, "0", opts)
      val stats = ZonalStatistics.zonalStatsLocal(farmlands.toArray(new Array[IFeature](0)), rasterReader, classOf[SoilStatistics])
      if (finalResults == null) finalResults = stats
      else for (i <- 0 until finalResults.length) {
        if (finalResults(i) == null) finalResults(i) = stats(i)
        else if (stats(i) != null) finalResults(i).accumulate(stats(i))
      }
    }

    // write results to json object// write results to json object
    val out = response.getWriter
    val mapper = new ObjectMapper

    // create query node// create query node
    val queryNode = mapper.createObjectNode
    queryNode.put("soildepth", soilDepth)
    queryNode.put("layer", layer)

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

    // create results node// create results node
    val resultsNode = mapper.createArrayNode

    // populate json object with max vals// populate json object with max vals
    for (i <- 0 until finalResults.length) {
      val s = finalResults(i).asInstanceOf[SoilStatistics]
      if (s != null) {
        val resultNode = mapper.createObjectNode
        resultNode.put("objectid", farmlands.get(i).getAs("OBJECTID").asInstanceOf[Number].longValue)
        resultNode.put("min", s.getMin)
        resultNode.put("max", s.getMax)
        resultNode.put("average", s.getAverage)
        resultNode.put("count", s.getCount)
        resultNode.put("stdev", s.getStdev)
        //resultNode.put("median", s.getMedian());
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

object SoilServlet {
  def rangeOverlap(r1: String, r2: String): Boolean = {
    val parts1 = r1.split("-")
    val begin1 = parts1(0).toInt
    val end1 = parts1(1).toInt
    val parts2 = r2.split("-")
    val begin2 = parts2(0).toInt
    val end2 = parts2(1).toInt
    // Treat both ranges as begin-inclusive and end-exclusive
    !(begin1 >= end2 || begin2 >= end1)
  }

  val rasterFiles: Map[String, String] = Map(
    "0-5" -> "0_5_compressed",
    "5-15" -> "5_15_compressed",
    "15-30" -> "15_30_compressed",
    "30-60" -> "30_60_compressed",
    "60-100" -> "60_100_compressed",
    "100-200" -> "100_200_compressed"
  )
}
