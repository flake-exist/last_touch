import java.text.SimpleDateFormat

object LastTouch_CONSTANTS {

  val USER_PATH:String = "user_path"
  val TIMELINE:String    = "timeline"

  val last_touch = (chain:Seq[String]) => {
    val res = chain.foldLeft(List.empty[Int]) {
      case (acc,i) if acc.isEmpty & i == "direct / (none)"   => List(0)
      case (acc,i) if acc.isEmpty & i != "direct / (none)"   => List(1)
      case (acc,i) if acc.sum > 0                            => acc :+ 0
      case (acc,i) if acc.sum ==0 & i == "direct / (none)"   => acc :+ 0
      case (acc,i) if acc.sum ==0 & i != "direct / (none)"   => acc :+ 1
      case _                                                 => Nil
    }
    val last_touch_values = res.reverse
    val control_sum       = last_touch_values.sum

    val last_touch_result = control_sum match {
      case 0 => last_touch_values.init :+ 1
      case _ => last_touch_values
    }

    last_touch_result
  }

  val unix_to_date = (unix_seq_str:String,ch_sep:String,format_template:String) => {
    val unix_seq_long:Seq[Long] = unix_seq_str.split(ch_sep).map(_.toLong)
    val date_format:SimpleDateFormat = new SimpleDateFormat(format_template)
    val date_seq:Seq[String] = unix_seq_long.map(date_format.format(_))
    date_seq
  }


}