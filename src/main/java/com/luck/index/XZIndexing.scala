package com.luck.index

import com.luck.curve.XZ2SFC
import com.luck.curve.hilbert.{HilbertCurve2D, HilbertCurve2DProvider}
import com.luck.curve.hilbert.sfcurve.SpaceFillingCurves
import org.locationtech.sfcurve.IndexRange

class XZIndexing {
  def index(g: Short, xmin: Double, ymin: Double, xmax: Double, ymax: Double): Long ={
    val sfc = XZ2SFC(g)
    sfc.index(xmin, ymin, xmax, ymax)
//    //用以下代码筛选范围
//    poly = sfc.index(xmin, ymin, xmax, ymax)
//    val ranges = sfc.ranges(Seq(bbox)).map(r => (r.lower, r.upper))
//    val matches = ranges.exists(r => r._1 <= poly && r._2 >= poly)
  }

  def ranges(g: Short, xmin: Double, ymin: Double, xmax: Double, ymax: Double): Seq[IndexRange] ={
    val sfc = XZ2SFC(g)
    sfc.ranges(xmin, ymin, xmax, ymax)
  }
}
