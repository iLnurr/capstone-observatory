package observatory

trait TimeCheck {
  def withTimeChecking[T](function: => (T)): T = {
    val before = System.currentTimeMillis()
    val result = function
    val after = System.currentTimeMillis()
    println(s"Method working duration=${after - before}")
    result
  }
}
