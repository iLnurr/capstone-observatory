package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math._
import Visualization._

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    val x = point.x
    val y = point.y
    d00 * (1 - x) * (1 - y) + d10 * x  * (1 - y) + d01 * (1 - x) * y + d11 * x * y
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {
    val imageWidth = 256
    val imageHeight = 256

    val pixels = pixelLocations(tile, imageWidth, imageHeight).par.map{
      case (pos,pixelLocation) => {

        val latRange = List(floor(pixelLocation.lat).toInt, ceil(pixelLocation.lat).toInt)
        val lonRange = List(floor(pixelLocation.lon).toInt, ceil(pixelLocation.lon).toInt)

        val d = {
          for {
            xPos <- 0 to 1
            yPos <- 0 to 1
          } yield (xPos, yPos) -> grid(GridLocation(latRange(1 - yPos), lonRange(xPos)))
        }.toMap

        val xFraction = pixelLocation.lon - lonRange.head
        val yFraction = latRange(1) - pixelLocation.lat


        pos -> interpolateColor(
          colors,
          bilinearInterpolation(point = CellPoint(xFraction, yFraction), d00 = d((0,0)), d01 = d((0,1)), d10 = d((1,0)), d11 = d((1,1)))
        ).pixel(127)
      }}
      .seq
      .sortBy(_._1)
      .map(_._2)


    Image(imageWidth, imageHeight, pixels.toArray)
  }

  /**
    * Generates a sequence of pos, location tuples for a Tile image
    * @param imageWidth in pixels
    * @param imageHeight in pixels
    * @return 'Map' of (pos->Location) within the tile
    */
  def pixelLocations(tile: Tile, imageWidth: Int, imageHeight: Int): IndexedSeq[(Int, Location)] = {
    for{
      xPixel <- 0 until imageWidth
      yPixel <- 0 until imageHeight
    } yield xPixel + yPixel * imageWidth -> Tile(xPixel.toDouble / imageWidth + tile.y, yPixel.toDouble / imageHeight + tile.y, tile.zoom).location
  }

}
