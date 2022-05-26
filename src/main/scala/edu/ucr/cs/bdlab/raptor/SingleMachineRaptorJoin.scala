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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}

import org.locationtech.jts.geom.Geometry

import scala.collection.mutable

object SingleMachineRaptorJoin {

  // statistics function
  def statistics(inputList: List[Float]): (java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Integer, java.lang.Float) = {
    var mode = new mutable.HashMap[Float, Int]()
    var max = Float.NegativeInfinity
    var min = Float.PositiveInfinity
    var sum = (0).toFloat
    for (j <- 0 until inputList.size) {
      val value = inputList(j)
      if (value > max)
        max = value
      if (value < min)
        min = value
      sum += value

      if (mode.contains(value)) {
        val x = mode.get(value).get
        mode.getOrElseUpdate(value, x + 1)
      }
      else {
        mode.put(value, 1)
      }
    }


    val count = inputList.size
    var median = Float.NegativeInfinity
    if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      median = (inputList(l) + inputList(r)).toFloat / 2
    } else
      median = inputList(count / 2).toFloat

    (max.asInstanceOf[java.lang.Float], min.asInstanceOf[java.lang.Float], median.asInstanceOf[java.lang.Float], sum.asInstanceOf[java.lang.Float], mutable.ListMap(mode.toSeq.sortWith(_._2 > _._2): _*).head._1.asInstanceOf[java.lang.Float], count.asInstanceOf[java.lang.Integer], (sum / count.toFloat).asInstanceOf[java.lang.Float])
  }

  // join function
  def join(rasterPath: String, geom: Geometry): (java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Float, java.lang.Integer, java.lang.Float) = {
    geom.setSRID(3857)
    val rasterFileNames: Array[String] = Array(rasterPath)
    val intersections: Array[Intersections] = rasterFileNames.map(rasterFileName => {
      val rasterFS: FileSystem = new Path(rasterFileName).getFileSystem(new Configuration())
      val rasterReader = RasterHelper.createRasterReader(rasterFS, new Path(rasterFileName), new BeastOptions(), new SparkConf())
      val intersections = new Intersections()
      intersections.compute(Array(geom), rasterReader.metadata, new BeastOptions())
      intersections
    })

    // compute min
    val minIntersectionIterator: Iterator[(scala.Long, PixelRange)] = new IntersectionsIterator(rasterFileNames.indices.toArray, intersections)
    val minPixelIterator: Iterator[RaptorJoinResult[scala.Float]] = new PixelIterator(minIntersectionIterator, rasterFileNames, "0")
    //val min = minPixelIterator.map(x => x.m).min.asInstanceOf[java.lang.Float]

    statistics(minPixelIterator.map(x => x.m).toList)

    // compute max
    //val maxIntersectionIterator: Iterator[(scala.Long, PixelRange)] = new IntersectionsIterator(rasterFileNames.indices.toArray, intersections)
    //val maxPixelIterator: Iterator[RaptorJoinResult[scala.Float]] = new PixelIterator(maxIntersectionIterator, rasterFileNames, "0")
    //val max = maxPixelIterator.map(x => x.m).max.asInstanceOf[java.lang.Float]

    // return statistics
    //(min, max)
  }
}
