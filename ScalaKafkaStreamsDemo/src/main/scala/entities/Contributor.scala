package entities

case class Contributor(email: String, ranking: Float, lastUpdatedTime: Long) {

  def updatedWithinLastMillis(currentTime: Long, limit: Long): Boolean = {
    currentTime - this.lastUpdatedTime <= limit
  }
}