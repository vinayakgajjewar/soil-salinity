package edu.ucr.cs.bdlab.raptor

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, WebMethod}
import edu.ucr.cs.bdlab.beast.indexing.RTreeFeatureReader
import edu.ucr.cs.bdlab.beast.io.{GeoJSONFeatureWriter, SpatialFileRDD}
import edu.ucr.cs.bdlab.beast.util.AbstractWebHandler
import edu.ucr.cs.bdlab.davinci.DaVinciServer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.io.{BufferedInputStream, File, PrintWriter}
import java.nio.file.Files
import java.util
import java.util.Iterator
import java.util.zip.GZIPOutputStream
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.collection.JavaConverters.asScalaIteratorConverter

class VectorServlet extends AbstractWebHandler with Logging {
  import VectorServlet._

  /** Additional options passed on by the user to override existing options */
  var opts: BeastOptions = _

  /** The SparkSession that is used to process datasets */
  var sparkSession: SparkSession = _

  /** The path at which this server keeps all datasets */
  var dataPath: String = _

  val daVinciServer: DaVinciServer = new DaVinciServer

  override def setup(ss: SparkSession, opts: BeastOptions): Unit = {
    super.setup(ss, opts)
    this.opts = opts
    this.sparkSession = ss
    this.dataPath = opts.getString("datapath", "data")
    this.daVinciServer.setup(ss, opts)
  }

  /**
   * Retrieves information about all the available features products.
   * The information is retrieved from a JSON file with a format similar to the following:
   *
   * <pre>
   * [
   * {
   * "id": "farmland",
   * "title": "California Farmlands",
   * "description": "All farmlands in California",
   * "extents": [-124.4096, 32.5343, -114.1312, 42.0095],
   * "index_path": "data/CA_farmland/index.rtree",
   * "viz_path": "data/CA_farmland/plot.zip"
   * }, {
   * ...
   * }
   * ]
   * </pre>
   */
  @WebMethod(url = "/vectors.json", method = "GET")
  def listDatasets(path: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    response.setContentType("application/json")
    // set Access-Control-Allow-Origin// set Access-Control-Allow-Origin
    // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy// otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
    response.addHeader("Access-Control-Allow-Origin", "*")
    if (!VectorIndexFile.exists) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      val writer = response.getWriter
      writer.printf("{'message': 'File %s not found'}", VectorIndexFile)
      writer.close()
      true
    } else {
      response.setStatus(HttpServletResponse.SC_OK)
      try {
        val input = new BufferedInputStream(Files.newInputStream(VectorIndexFile.toPath))
        try {
          val objectMapper = new ObjectMapper
          val rootNode = objectMapper.readTree(input)
          // Create a new ObjectNode and put the read JsonNode under the "vectors" attribute
          val wrapperNode = objectMapper.createObjectNode
          wrapperNode.set("vectors", rootNode)
          // Write the wrapper ObjectNode to the output stream
          val out = response.getOutputStream
          objectMapper.writerWithDefaultPrettyPrinter.writeValue(out, wrapperNode)
          out.close()
          true
        } finally if (input != null) input.close()
      }
    }
  }

  @WebMethod(url = "/vectors/{datasetID}.geojson", method = "GET")
  def retrieveDataset(path: String, request: HttpServletRequest, response: HttpServletResponse, datasetID: String): Boolean = {
    // time at start of GET request
    val t1 = System.nanoTime

    // we set content-type as application/geo+json// we set content-type as application/geo+json
    // not application/json// not application/json
    response.setContentType("application/geo+json")

    // set Access-Control-Allow-Origin// set Access-Control-Allow-Origin
    // otherwise, the front-end won't be able to make GET requests to this server because of CORS policy// otherwise, the front-end won't be able to make GET requests to this server because of CORS policy
    response.addHeader("Access-Control-Allow-Origin", "*")

    // Locate the index path// Locate the index path
    val dataIndexPath = getIndexPathById(VectorIndexFile, datasetID)
    if (dataIndexPath == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      val writer = response.getWriter
      writer.printf("{'message': 'Cannot find dataset %s'}", datasetID)
      return true
    }

    response.setStatus(HttpServletResponse.SC_OK)
    // Load the Farmland features// Load the Farmland features
    val reader = new RTreeFeatureReader
    val indexPath = new Path(dataIndexPath)

    val fs = indexPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val fileLength = fs.getFileStatus(indexPath).getLen
    val inputFileSplit = new FileSplit(indexPath, 0, fileLength, null)

    val opts = new BeastOptions
    try {
      // get extents parameters
      val minx = request.getParameter("minx").toDouble
      val miny = request.getParameter("miny").toDouble
      val maxx = request.getParameter("maxx").toDouble
      val maxy = request.getParameter("maxy").toDouble
      opts.set(SpatialFileRDD.FilterMBR, Array(minx, miny, maxx, maxy).mkString(","))
    } catch {
      case e: NullPointerException =>
      // MBR not passed. Use all features
    }

    reader.initialize(inputFileSplit, opts)

    val gzipRequest = isGZIPAcceptable(request)
    if (gzipRequest) response.setHeader("Content-Encoding", "gzip")

    // try writing out a record// try writing out a record
    var numRecords = 0
    try {
      val writer = new GeoJSONFeatureWriter
      try {
        writer.initialize(if (gzipRequest) new GZIPOutputStream(response.getOutputStream)
        else response.getOutputStream, new Configuration)
        for (feature <- reader.asScala) {
          writer.write(feature)
          numRecords += 1
        }
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
      } finally if (writer != null) writer.close()
    }
    reader.close()

    // time at end of GET request// time at end of GET request
    val t2 = System.nanoTime

    logInfo(s"Vector dataset retrieved $numRecords records in ${(t2-t1)*1E-9} seconds")
    true
  }

  @WebMethod(url = "/vectors/{datasetID}/tile-{z}-{x}-{y}.{ext}", method = "GET")
  def retrieveDataset(path: String, request: HttpServletRequest, response: HttpServletResponse,
                      datasetID: String, z: Int, x: Int, y: Int, ext: String): Boolean = {
    response.addHeader("Access-Control-Allow-Origin", "*")
    val vizPath = getVizPathById(VectorIndexFile, datasetID)
    if (vizPath == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
      return true
    }

    val redirectedPath = s"/dynamic/visualize.cgi/$vizPath/tile-$z-$x-$y.$ext"
    daVinciServer.getTile(redirectedPath, request, response, vizPath, z, x, y, ext)
  }
}

object VectorServlet {
  val VectorIndexFile = new File("data/vectors.json")

  @throws[java.io.IOException]
  def getIndexPathById(filePath: File, id: String): String = {
    val objectMapper: ObjectMapper = new ObjectMapper
    val rootNode: JsonNode = objectMapper.readTree(filePath)
    if (rootNode.isArray) {
      val elements: util.Iterator[JsonNode] = rootNode.elements
      while (elements.hasNext) {
        val element: JsonNode = elements.next
        if (element.get("id").asText == id) return element.get("index_path").asText
      }
    }
    null // ID not found
  }

  def isGZIPAcceptable(request: HttpServletRequest): Boolean = {
    var serverAcceptGZIP = false
    val serverAccepts = request.getHeaders("Accept-Encoding")
    while (serverAccepts.hasMoreElements) {
      val serverAccept = serverAccepts.nextElement.asInstanceOf[String]
      if (serverAccept.toLowerCase.contains("gzip")) serverAcceptGZIP = true
    }
    serverAcceptGZIP
  }

  @throws[java.io.IOException]
  def getVizPathById(filePath: File, id: String): String = {
    val objectMapper = new ObjectMapper
    val rootNode = objectMapper.readTree(filePath)
    if (rootNode.isArray) {
      val elements = rootNode.elements
      while (elements.hasNext) {
        val element = elements.next
        if (element.get("id").asText == id) return element.get("viz_path").asText
      }
    }
    null // ID not found

  }
}
