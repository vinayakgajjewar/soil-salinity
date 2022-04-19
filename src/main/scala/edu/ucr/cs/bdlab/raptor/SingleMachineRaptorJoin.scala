/*
 * Copyright 2021 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.IFeature
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf;
import org.apache.hadoop.fs.{FileSystem, Path}

object SingleMachineRaptorJoin {
//class SingleMachineRaptorJoin {
  //def join(args: Array[String]): Unit = {
  def join(): Unit = {
    val conf = new SparkConf()
    val vectorFileName: String = "tl_2018_us_state.zip"
    val rasterFileNames: Array[String] = Array("glc2000_v1_1.tif")
    val inputVector: Array[IFeature] = SpatialFileRDD.readLocal(vectorFileName, "shapefile",
      new BeastOptions(), new Configuration()).toArray
    val intersections: Array[Intersections] = rasterFileNames.map(rasterFileName => {
      val rasterFS: FileSystem = new Path(rasterFileName).getFileSystem(new Configuration())
      val rasterReader = RasterHelper.createRasterReader(rasterFS, new Path(rasterFileName), new BeastOptions(), new SparkConf())
      val intersections = new Intersections()
      intersections.compute(inputVector.map(_.getGeometry), rasterReader.metadata, new BeastOptions())
      intersections
    })
    val intersectionIterator: Iterator[(Long, PixelRange)] = new IntersectionsIterator(rasterFileNames.indices.toArray, intersections)
    val pixelIterator: Iterator[RaptorJoinResult[Long]] = new PixelIterator(intersectionIterator, rasterFileNames, "0")
  }
}
