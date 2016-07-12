class ChatContent(val date:String, val time:String, val sender:String, var chatText:String) extends java.io.Serializable {
    def this(specialChat: String) = this(specialChat, specialChat, specialChat, specialChat)
    def appendToChatContent(chatFragment: String) {
      chatText = chatText.concat("\n").concat(chatFragment)
    }
  }
class ProcessChats extends java.io.Serializable {
  val MEDIA_CONSTANT = "<Media omitted>"
  val WHATSAPP_NOTIFICATION = "Whatsapp Notification"
  val LIST_OF_ADULT_WORDS: List[String] = List("fuck", "fucking", "sex", "boobs", "dick", "pussy", "ass")
  val LIST_OF_FILTERED_WORDS: List[String] = List("<Media", "omitted>", "previous___chat", "")
  val DATE_LIMIT = 5
  val TIME_LIMIT = 5
  val SENDER_LIMIT = 50
  val CHAT_LIMIT = 5
  val WORD_LIMIT = 5
  val LONG_WORD_LIMIT = 10
  val SMILEY_LIMIT = 10
  val ADULT_WORD_LIMIT = 10
  
  val DATE_LIMIT_INDIVIDUAL = 3
  val TIME_LIMIT_INDIVIDUAL = 3
  val SENDER_LIMIT_INDIVIDUAL = 1
  val CHAT_LIMIT_INDIVIDUAL = 3
  val WORD_LIMIT_INDIVIDUAL = 3
  val LONG_WORD_LIMIT_INDIVIDUAL = 4
  val SMILEY_LIMIT_INDIVIDUAL = 5
  val ADULT_WORD_LIMIT_INDIVIDUAL = 5
  
  val SMILEY_REGEX = """(😊|😌|😞|😀|😝|😜|😘|😍|😋|😂|😁|😉|😐|😭|😳|😬|😄|😇){1,}""".r
  
  val GROUP_COMBINED_LONGEST_STREAK_INFO = "\nLongest streak between the members of the group was for "
  val GROUP_INDIVIDUAL_LONGEST_STREAK_INFO = "\nYour longest streak in the group was for "
  val INDIVIDUAL_COMBINED_LONGEST_STREAK_INFO = "\nLongest streak between you two was for "
  val INDIVIDUAL_INDIVIDUAL_LONGEST_STREAK_INFO = "\nYour longest streak between you was for "
  
  val GROUP_COMBINED_SILENT_STREAK_INFO = "\nLongest span of silent days between the members of the group was for "
  val GROUP_INDIVIDUAL_SILENT_STREAK_INFO = "\nYour longest span of silent days in the group was for "
  val INDIVIDUAL_COMBINED_SILENT_STREAK_INFO = "\nLongest span of silent days between you was for "
  val INDIVIDUAL_INDIVIDUAL_SILENT_STREAK_INFO = "\nYour longest span of silent days between you was for "
  
  val INFO_UNAVAILABLE = "unavailable"
  
  def divideContent(line:String): ChatContent = {
      var content = line.replaceFirst(",", "date___delimiter")
      content = content.replaceFirst("-", "time___delimiter")
      content = content.replaceFirst(":", "sender___first___delimiter")
      content = content.replaceFirst(":", "sender___second___delimiter")
      var date = ""
      var time = ""
      var sender = ""
      var chatContent = ""
    
      if(content.split("date___delimiter").length == 1) { date = "unavailable"} else { date = content.split("date___delimiter")(0).trim }
    
      if(content.split("date___delimiter").length < 2)  { time = "unavailable";  } else { time = content.split("date___delimiter")(1).split("time___delimiter")(0).replace("sender___first___delimiter",":").trim }
    
      if(content.split("time___delimiter").length < 2) { sender = "unavailable" } else { 
        if(content.split("sender___second___delimiter").length < 2) {
          sender = WHATSAPP_NOTIFICATION
          chatContent = content.split("time___delimiter")(1).split("sender___second___delimiter")(0).trim
        }
        else {
          sender = content.split("time___delimiter")(1).split("sender___second___delimiter")(0).trim 
        }
      }
     
      if(chatContent=="") {
        if(content.split("sender___second___delimiter").length < 2) { chatContent = "unavailable" }
        else { chatContent = content.split("sender___second___delimiter")(1).trim }
      }
    return new ChatContent(date, time, sender, chatContent)
  }
  
  def parseContent(line:String): ChatContent = {
    val datePattern = "[0-9][0-9]?/[0-9][0-9]?/[0-9][0-9]?,.*"
    if(!line.matches(datePattern)) 
      return new ChatContent("previous___chat")
    return this.divideContent(line)
  }
  
  def processChats(filePath: String): List[ChatContent] = {
    val baseFile = sc.textFile(filePath)
    val allLines = baseFile.map(eachLine => eachLine).cache().collect()
    var allChats = List(new ChatContent("previous___chat"))
    for(line <- allLines) {
      val eachChat = this.parseContent(line)
      if(eachChat.chatText=="previous___chat")
        allChats.last.appendToChatContent(line)
      else 
        allChats = allChats:+eachChat
    }
    return allChats
  }
  
  def checkMedia(word: String): (String,Int) = word match {
      case MEDIA_CONSTANT => (word,1)
      case _ => (word,0)
  }
  
  def checkSmileys(word: String): (String, Int) =  word match {
      case SMILEY_REGEX(word) => (word,1)
      case _ => (word, 0)
  }
    
  def checkWords(word: String): (String,Int) = word match {
      case word if(LIST_OF_ADULT_WORDS.contains(word)) => (word, 1)
      case _ => (word,0)
  }
    
  def checkLongWords(word: String): (String,Int) = {
      if(word.length > 4) 
        return (word,1)
      return (word,0)
  }
  
  def filterOutWords(word: String): Boolean = {
    return !LIST_OF_FILTERED_WORDS.contains(word)
  }
  
  def filterOutChatContent(chatContent: ChatContent): Boolean = {
    return !LIST_OF_FILTERED_WORDS.contains(chatContent.date)
  }
  
  def silentDays(dateArray: Array[String]): (Long, String, String) = {
      import util.control.Breaks._
      var maxDays: Long = 1
      var fromDay = ""
      var toDay = ""
      for(i <- 0 to dateArray.length-2) {
        breakable {
          if((!dateArray(i).matches("[0-9/0-9/0-9].*")) || (!dateArray(i+1).matches("[0-9/0-9/0-9].*"))) {
            break
          }
          
          val difference = findDifference(dateArray(i).replaceAll("/","-"), dateArray(i+1).replaceAll("/","-"))
          if(difference > maxDays) {
            maxDays = difference
            fromDay = dateArray(i)
            toDay = dateArray(i+1)
          }
        }
      }
      return (maxDays, fromDay, toDay)
    }
    
    def findConsecutiveStreak(dateArray: Array[String]): (Long, String, String) = {
      import util.control.Breaks._
      var maxDays: Int = 1
      var streakEnd: String = dateArray(0)
      var streakBeginLocal: String = dateArray(0)
      var streakBeginGlobal: String = dateArray(0)
      var runningCount = 1
      for(i <- 0 to dateArray.length-2) {
        breakable {
          if((!dateArray(i).matches("[0-9/0-9/0-9].*")) || (!dateArray(i+1).matches("[0-9/0-9/0-9].*"))) {
            break
          }
          
          val difference = findDifference(dateArray(i).replaceAll("/","-"), dateArray(i+1).replaceAll("/","-"))
          if(difference == 1) {
            runningCount = runningCount+1
            if(runningCount > maxDays) {
              maxDays = runningCount
              streakEnd = dateArray(i+1)
              streakBeginGlobal = streakBeginLocal
            }
          }
          else if(difference >1){
            runningCount = 1
            streakBeginLocal = dateArray(i+1)
          }
        }
      }
      return (maxDays, streakBeginGlobal, streakEnd)
    }
    
    
    def findDifference(startDate:String, endDate:String): Long = {
      import java.time.LocalDate 
      import java.time.format.DateTimeFormatter
      
      val formatter = DateTimeFormatter.ofPattern("M-d-yy")
      try {
        val startDateFormatted = LocalDate.parse(startDate, formatter)
        val endDateFormatted = LocalDate.parse(endDate, formatter)
        val diff = endDateFormatted.toEpochDay() - startDateFormatted.toEpochDay()
        return diff
      } catch {
        case e: Exception => return 0
      }
    }
  
    def loadInfoString(isGroup: Boolean, statType: String, infoFor: String): String = (isGroup, statType, infoFor) match {
      case (true, "combined", "longest") => GROUP_COMBINED_LONGEST_STREAK_INFO
      case (true, "individual", "longest") => GROUP_INDIVIDUAL_LONGEST_STREAK_INFO
      case (false, "combined", "longest") => INDIVIDUAL_COMBINED_LONGEST_STREAK_INFO
      case (false, "individual", "longest") => INDIVIDUAL_INDIVIDUAL_LONGEST_STREAK_INFO
      case (true, "combined", "silent") => GROUP_COMBINED_SILENT_STREAK_INFO
      case (true, "individual", "silent") => GROUP_INDIVIDUAL_SILENT_STREAK_INFO
      case (false, "combined", "silent") => INDIVIDUAL_COMBINED_SILENT_STREAK_INFO
      case (false, "individual", "silent") => INDIVIDUAL_INDIVIDUAL_SILENT_STREAK_INFO
      case _ => INFO_UNAVAILABLE
    }
  
  def calculateStats(chatRDD:org.apache.spark.rdd.RDD[ChatContent], isGroup: Boolean, statType: String, limitedStats: Integer) {
    
      def isCombined: Boolean = statType=="combined"
      println("\nSender Stats: (sender : number of chats)")
      val senderStats = chatRDD.filter(filterOutChatContent).map(each=>(each.sender,1)).reduceByKey(_+_).collect().sortWith(_._2 > _._2).take({
        if(isCombined) SENDER_LIMIT
        else SENDER_LIMIT_INDIVIDUAL
      }).foreach(println)
    
      println("\nMedia Stats: Number of media files sent")
      val mediaStats = chatRDD.filter(_.chatText==MEDIA_CONSTANT).count()
      println(mediaStats)
    
      println("\nDate Stats: (date : number of chats)")
      val dateStats = chatRDD.map(each=>(each.date,1)).reduceByKey(_+_).collect().sortWith(_._2 > _._2).take({
        if(isCombined) DATE_LIMIT
        else DATE_LIMIT_INDIVIDUAL
      }).foreach(println)
      
      println("\nTime Stats: (time : number of chats)")
      val timeStats = chatRDD.map(each=>(each.time,1)).reduceByKey(_+_).collect().sortWith(_._2 > _._2).take({
        if(isCombined) TIME_LIMIT
        else TIME_LIMIT_INDIVIDUAL
      }).foreach(println)
    
      println("\nText Stats: (text : number of times sent)")
      val chatTextStats = chatRDD.filter(_.chatText!=MEDIA_CONSTANT).map(each=>(each.chatText,1)).reduceByKey(_+_).collect().sortWith(_._2 > _._2).take({
        if(isCombined) CHAT_LIMIT
        else CHAT_LIMIT_INDIVIDUAL
      }).foreach(println)
    
      println("\nWord Stats: (word:number of times sent)")
      val allWordsStats = chatRDD.flatMap(each=>each.chatText.split(" ")).filter(filterOutWords).map(word=>(word,1)).reduceByKey(_+_).collect().sortWith(_._2 > _._2).take({
        if(isCombined) WORD_LIMIT
        else WORD_LIMIT_INDIVIDUAL
      }).foreach(println)
    
      println("\nLong Word Stats: (word:number of times sent)")
      val longWordsStats = chatRDD.flatMap(each=>each.chatText.split(" ")).filter(filterOutWords).map(checkLongWords).reduceByKey(_+_).collect().sortWith(_._2 > _._2).take({
        if(isCombined) LONG_WORD_LIMIT
        else LONG_WORD_LIMIT_INDIVIDUAL
      }).foreach(println)
    
      println("\nAdult Word Stats: (word:number of times sent)")
      val fWordStats = chatRDD.flatMap(each=>each.chatText.split(" ")).map(checkWords).reduceByKey(_+_).collect().sortWith(_._2 > _._2).take({
        if(isCombined) ADULT_WORD_LIMIT
        else ADULT_WORD_LIMIT_INDIVIDUAL
      }).foreach((a) => {
          if(a._2>0) println(a)
      })
      
      println("\nSmiley Stats: (smiley:number of times sent)")
      val smileyStats = chatRDD.flatMap(each=>each.chatText.split(" ")).map(checkSmileys).reduceByKey(_+_).collect().sortWith(_._2 > _._2).take({
        if(isCombined) SMILEY_LIMIT
        else SMILEY_LIMIT_INDIVIDUAL
      }).foreach((a) => {
          if(a._2>0) println(a)
      })
      
      /* Common Stats */
      if(limitedStats == -1) {
        var infoString: String = loadInfoString(isGroup, statType, "longest")
        val longestStreakDays = this.findConsecutiveStreak(chatRDD.filter(filterOutChatContent).map(each=>(each.date)).collect())
        println(infoString + longestStreakDays._1 + " days from " + longestStreakDays._2 + " to " + longestStreakDays._3)
    
        infoString = loadInfoString(isGroup, statType, "silent")
        val silentDays = this.silentDays(chatRDD.filter(filterOutChatContent).map(each=>(each.date)).collect())
        println(infoString + silentDays._1 + " days from " + silentDays._2 + " to " + silentDays._3 + ".")
      }
  }
  
  def mainRun(filePath:String, isGroup:Boolean) {
    val allChats = this.processChats(filePath)
    val chatRDD = sc.parallelize(allChats)
    println("\nALL STATS \n=====================")
    var statType = "combined"
    var limitedStats = -1
    this.calculateStats(chatRDD, isGroup, statType, limitedStats)
    println("\n=========================")
    /* Individual Stats */
    println("\nINDIVIDUAL STATS \n==========================")
    val senderList = chatRDD.filter(filterOutChatContent).map(each=>(each.sender)).distinct.collect().sorted
    statType = "individual"
    for(sender <- senderList) {
      limitedStats = -1
      println("Stats of " + sender + "\n=======================")
      if(sender=="Whatsapp Notification") {
        limitedStats = 1
      }
      val senderRDD = chatRDD.filter(_.sender==sender)
      this.calculateStats(senderRDD, isGroup, statType, limitedStats)
      println("\n=========================")
    }
  }
}
