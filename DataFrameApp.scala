import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.functions.{avg, count, max, sum}
import org.apache.spark.sql.{SaveMode, SparkSession}


object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val strpath = "C:\\Users\\Chardon\\Documents\\AADoc\\ITMO\\BigData\\DM\\bgdata_small\\";
    //val strpath = "/data/bgdata_small/";


    val userWallComments = strpath + "userWallComments.parquet";
    val userWallLikes = strpath + "userWallLikes.parquet";
    val userWallPosts = strpath + "userWallPosts.parquet";

    val userWallProfiles = strpath + "userWallProfiles.parquet";
    val userWallPhotosLikes = strpath + "userWallPhotosLikes.parquet";
    val userWallPhotos = strpath + "userWallPhotos.parquet";

    val userGroupsSubs =  strpath + "userGroupsSubs.parquet";

    val followerProfiles = strpath + "followerProfiles.parquet";
    val friendsProfiles = strpath + "friendsProfiles.parquet";
    val groupsProfiles = strpath + "groupsProfiles.parquet";
    val commonUserProfiles = strpath + "commonUserProfiles.parquet";

    val followers = strpath + "followers.parquet";
    val friends = strpath + "friends.parquet";
    val likes = strpath + "likes.parquet";
    val traces = strpath + "traces.parquet";


    val spark = SparkSession.builder()
      .appName("HomeworkTytgatKarel")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

/*
    // Question 1
    val q1SQLTimeS = System.nanoTime();  val q1SQL = question1SQL(spark, userWallComments, userWallLikes, userWallPosts); writeParquetToComputer(q1SQL,strpath + "Questions\\Q1SQL");      val q1SQLTimeE = System.nanoTime();  val q1SQLExeTime = (q1SQLTimeE-q1SQLTimeS) / 1e9d
    val q1DFTimeS = System.nanoTime();   val q1DF = question1DF(spark, userWallComments, userWallLikes, userWallPosts); writeParquetToComputer(q1DF,strpath + "Questions\\Q1DF");          val q1DFTimeE = System.nanoTime();   val q1DFExeTime = (q1DFTimeE-q1DFTimeS) / 1e9d
    val q1APITimeS = System.nanoTime();  val q1API = question1API(spark, userWallComments, userWallLikes, userWallPosts); q1API.foreach(println) ;                                         val q1APITimeE = System.nanoTime();  val q1APIExeTime = (q1APITimeE-q1APITimeS) / 1e9d
    printTimetable(1,q1SQLExeTime,q1DFExeTime,q1APIExeTime)
*/

    // Question 2
    //val q2SQLTimeS = System.nanoTime();  val q2SQL = question2SQL(spark, friends, followers, userGroupsSubs, userWallPhotos); writeParquetToComputer(q2SQL,strpath + "Questions\\Q2SQL");  val q2SQLTimeE = System.nanoTime(); val q2SQLExeTime = (q2SQLTimeE-q2SQLTimeS) / 1e9d
    //val q2DFTimeS = System.nanoTime();  val q2DF = question2DF(spark, friends, followers, userGroupsSubs, userWallPhotos); writeParquetToComputer(q2DF,strpath + "Questions\\Q2DF");      val q2DFTimeE = System.nanoTime();  val q2DFExeTime = (q2DFTimeE-q2DFTimeS) / 1e9d
    //printTimetable(2,q2SQLExeTime,q2DFExeTime,123456789)

/*
    // Question 3
    val q3SQLTimeS = System.nanoTime();  val q3SQL = question3SQL(spark, userWallComments); writeParquetToComputer(q3SQL,strpath + "Questions\\Q3SQL");                                    val q3SQLTimeE = System.nanoTime(); val q3SQLExeTime = (q3SQLTimeE-q3SQLTimeS) / 1e9d
    val q3DFTimeS = System.nanoTime();  val q3DF = question3DF(spark, userWallComments); writeParquetToComputer(q3DF,strpath + "Questions\\Q3DF");                                        val q3DFTimeE = System.nanoTime();  val q3DFExeTime = (q3DFTimeE-q3DFTimeS) / 1e9d
    printTimetable(3,q3SQLExeTime,q3DFExeTime,123456789)
*/
/*
    //Question 4
    val q4SQLTimeS = System.nanoTime();  val q4SQL = question4SQL(spark, userWallLikes); writeParquetToComputer(q4SQL,strpath + "Questions\\Q4SQL");                                       val q4SQLTimeE = System.nanoTime(); val q4SQLExeTime = (q4SQLTimeE-q4SQLTimeS) / 1e9d
    val q4DFTimeS = System.nanoTime();  val q4DF = question4DF(spark, userWallLikes); writeParquetToComputer(q4DF,strpath + "Questions\\Q4DF");                                           val q4DFTimeE = System.nanoTime();  val q4DFExeTime = (q4DFTimeE-q4DFTimeS) / 1e9d
    printTimetable(4,q4SQLExeTime,q4DFExeTime,123456789)
*/
/*
    //Question 5
    val q5SQLTimeS = System.nanoTime();  val q5SQL = question5SQL(spark, groupsProfiles, userGroupsSubs); writeParquetToComputer(q5SQL,strpath + "Questions\\Q5SQL");                      val q5SQLTimeE = System.nanoTime(); val q5SQLExeTime = (q5SQLTimeE-q5SQLTimeS) / 1e9d
    val q5DFTimeS = System.nanoTime();  val q5DF = question5DF(spark, groupsProfiles, userGroupsSubs); writeParquetToComputer(q5DF,strpath + "Questions\\Q5DF");                          val q5DFTimeE = System.nanoTime();  val q5DFExeTime = (q5DFTimeE-q5DFTimeS) / 1e9d
    printTimetable(5,q5SQLExeTime,q5DFExeTime,123456789)
*/
/*
    //Question 6
    val q6SQLTimeS = System.nanoTime();  val q6SQL = question6SQL(spark, userWallProfiles, friends, followers); writeParquetToComputer(q6SQL,strpath + "Questions\\Q6SQL");                val q6SQLTimeE = System.nanoTime(); val q6SQLExeTime = (q6SQLTimeE-q6SQLTimeS) / 1e9d
    val q6DFTimeS = System.nanoTime();  val q6DF = question6DF(spark, userWallProfiles, friends, followers); writeParquetToComputer(q6DF,strpath + "Questions\\Q6DF");                    val q6DFTimeE = System.nanoTime();  val q6DFExeTime = (q6DFTimeE-q6DFTimeS) / 1e9d
    printTimetable(6,q6SQLExeTime,q6DFExeTime,123456789)
*/
/*
    //Crash on question 7
    //18/12/16 17:11:24 ERROR BroadcastExchangeExec: Could not execute broadcast in 300 secs.
    //Question 7
    val q7SQLTimeS = System.nanoTime();  val q7SQL = question7SQL(spark, userWallComments, userWallLikes, followers, friends); writeParquetToComputer(q7SQL,strpath + "Questions\\Q7SQL"); val q7SQLTimeE = System.nanoTime(); val q7SQLExeTime = (q7SQLTimeE-q7SQLTimeS) / 1e9d
    val q7DFTimeS = System.nanoTime();  val q7DF = question7DF(spark, userWallComments, userWallLikes, followers, friends); writeParquetToComputer(q7DF,strpath + "Questions\\Q7DF");     val q7DFTimeE = System.nanoTime();  val q7DFExeTime = (q7DFTimeE-q7DFTimeS) / 1e9d
    printTimetable(7,q7SQLExeTime,q7DFExeTime,123456789)
*/
    /*
        //Question 9
        val q9SQLTimeS = System.nanoTime();  val q9SQL = question9SQL(spark, strpath); writeParquetToComputer(q9SQL,strpath + "Questions\\Q9SQL"); val q9SQLTimeE = System.nanoTime(); val q9SQLExeTime = (q9SQLTimeE-q9SQLTimeS) / 1e9d
        val q9DFTimeS = System.nanoTime();  val q9DF = question9DF(spark, strpath); writeParquetToComputer(q9DF,strpath + "Questions\\Q9DF"); val q9DFTimeE = System.nanoTime(); val q9DFExeTime = (q9DFTimeE-q9DFTimeS) / 1e9d
        printTimetable(9,q9SQLExeTime,q9DFExeTime,123456789)
    */
  }


  def printTimetable(questionNb:Integer, SQLExeTime:Double, DFExeTime:Double ,APIExeTime:Double): Unit = {
    val strbestTime = bestTime(SQLExeTime,DFExeTime,APIExeTime);
    var APIExeTimeStr = APIExeTime + "";
    if(APIExeTime == 123456789){
      APIExeTimeStr = "";
    }
    println("|task\t| spark native api time \t| spark dataframe api \t| spark sql api \t| best \t|")
    println("|"+ questionNb + "\t| " + APIExeTimeStr + "|\t" + DFExeTime + "\t| " + SQLExeTime + "\t|" + strbestTime + "\t|" );
  }

  def writeParquetToComputer(dataFrame: sql.DataFrame, name: String): Unit ={
    dataFrame.write.mode(SaveMode.Overwrite).parquet(name);
  }

  def bestTime(sql: Double, df: Double, api: Double): String = {
    if(sql < df && sql < api){
      return "SQL";
    }
    if(df < api && df < sql){
      return "DataFrame";
    }
    if(api < sql && api < df){
      return "native"
    }
    return "";
  }

  def question1SQL(spark: SparkSession, userWallComments: String, userWallLikes: String, userWallPosts: String): sql.DataFrame ={

    spark.read.parquet(userWallComments).createOrReplaceTempView("userWallComments")
    spark.read.parquet(userWallLikes).createOrReplaceTempView("userWallLikes")
    spark.read.parquet(userWallPosts).createOrReplaceTempView("userWallPosts")


    //spark.sql("SELECT count(itemId) AS Likes, likerId AS UserID               FROM userWallLikes      GROUP BY UserID").show();
    //spark.sql("SELECT count(id) AS Post ,owner_id AS UserID                   FROM userWallPosts      GROUP BY UserID").show()
    //spark.sql("SELECT COUNT(post_owner) AS Comment, post_owner_id AS UserID   FROM userWallComments   GROUP BY UserID").show();

    val retour = spark.sql("SELECT Like.UserID, Likes, Post, Comment FROM " +
      " (SELECT count(itemId)     AS Likes, likerId AS UserID           FROM userWallLikes      GROUP BY UserID)  AS Like    JOIN " +
      " (SELECT count(id)         AS Post ,owner_id AS UserID           FROM userWallPosts      GROUP BY UserID)  AS Post    ON Like.UserID = Post.UserID JOIN " +
      " (SELECT COUNT(post_id)    AS Comment, post_owner_id AS UserID   FROM userWallComments   GROUP BY UserID)  AS Comment ON Post.UserID = Comment.UserID ")

    return retour;
  }

  def question2SQL(spark: SparkSession, friends: String, followers: String, userGroupsSubs: String, userWallPhotos: String): sql.DataFrame = {

    spark.read.parquet(followers).createOrReplaceTempView("followers")
    spark.read.parquet(friends).createOrReplaceTempView("friends")
    spark.read.parquet(userGroupsSubs).createOrReplaceTempView("userGroupsSubs")
    spark.read.parquet(userWallPhotos).createOrReplaceTempView("userWallPhotos")

    /*
    spark.sql("SELECT count(follower) AS Follower, profile AS UserID    FROM  followers                                   GROUP BY UserID ").show()
    spark.sql("SELECT count(follower) AS Friend, profile AS UserID      FROM  friends                                     GROUP BY UserID ").show()
    spark.sql("SELECT count(group)    AS Group, user AS UserID          FROM  userGroupsSubs                              GROUP BY UserID ").show()
    spark.sql("SELECT count(photo_75) AS Photo, owner_id AS UserID      FROM  userWallPhotos  WHERE photo_75 LIKE '%.jpg' GROUP BY UserID ").show()
    spark.sql("SELECT count(photo_75) AS Gif, owner_id AS UserID        FROM  userWallPhotos  WHERE photo_75 LIKE '%.gif' GROUP BY UserID ").show()
    */

    spark.sql("SELECT Followers.UserID, Follower, Friend, Group, Photo, Gif FROM " +
      " (SELECT count(follower) AS Follower, profile AS UserID    FROM  followers                                   GROUP BY UserID ) AS Followers JOIN " +
      " (SELECT count(follower) AS Friend, profile AS UserID      FROM  friends                                     GROUP BY UserID ) AS Friends   ON Followers.UserID = Friends.UserID JOIN " +
      " (SELECT count(group)    AS Group, user AS UserID          FROM  userGroupsSubs                              GROUP BY UserID ) AS Groups    ON Friends.UserID = Groups.UserID JOIN " +
      " (SELECT count(photo_75) AS Photo, owner_id AS UserID      FROM  userWallPhotos  WHERE photo_75 LIKE '%.jpg' GROUP BY UserID ) AS Photos    ON Groups.UserID = Photos.UserID JOIN " +
      " (SELECT count(photo_75) AS Gif, owner_id AS UserID        FROM  userWallPhotos  WHERE photo_75 LIKE '%.gif' GROUP BY UserID ) AS Gifs      ON Photos.UserID = Gifs.UserID ")

  }

  def question3SQL(spark: SparkSession, userWallComments: String): sql.DataFrame = {

    spark.read.parquet(userWallComments).createOrReplaceTempView("userWallComments")
    spark.sql("" +
      " SELECT PostID, AVG(NbPost) AS AVGPost, MAX(NbPost) AS MAXPost, SUM(NbPost) AS NbOfPost FROM " +
      "    (SELECT UserID,PostID, count(PostID) AS NbPost FROM " +
      "       (SELECT from_id AS UserID, post_id AS PostID FROM userWallComments WHERE  from_id <> post_owner_id ORDER BY UserID)" +
      "    GROUP BY UserID,PostID ORDER BY UserID )" +
      " GROUP BY PostID")
  }

  def question4SQL(spark: SparkSession, userWallLikes: String): sql.DataFrame = {

    spark.read.parquet(userWallLikes).createOrReplaceTempView("userWallLikes")
    spark.sql("" +
      " SELECT PostID, AVG(NbLike) AS AVGLike, MAX(NbLike) AS MAXLike, SUM(NbLike) AS NbOfLike FROM " +
      "    (SELECT UserID,PostID, count(PostID) AS NbLike FROM " +
      "       (SELECT likerId AS UserID, itemId AS PostID FROM userWallLikes WHERE  likerId <> ownerId ORDER BY PostID)" +
      "    GROUP BY UserID,PostID ORDER BY UserID )" +
      " GROUP BY PostID")
  }

  def question5SQL(spark: SparkSession, groupsProfiles: String, userGroupsSubs : String): sql.DataFrame = {
    spark.read.parquet(groupsProfiles).createOrReplaceTempView("groupsProfiles")
    spark.read.parquet(userGroupsSubs).createOrReplaceTempView("userGroupsSubs")

    //spark.sql("SELECT count(groupsProfiles.key) AS NotClosedGroup, user FROM groupsProfiles JOIN userGroupsSubs ON groupsProfiles.key = group WHERE is_closed = 0 GROUP BY user").show()
    //spark.sql("SELECT count(groupsProfiles.key) AS ClosedGroup, user    FROM groupsProfiles JOIN userGroupsSubs ON groupsProfiles.key = group WHERE is_closed = 1 GROUP BY user").show()

    spark.sql("SELECT NotClosedGroup, ClosedGroup, closed.user AS UserID FROM" +
      " (SELECT count(groupsProfiles.key) AS NotClosedGroup, user FROM groupsProfiles JOIN userGroupsSubs ON groupsProfiles.key = group WHERE is_closed = 0 GROUP BY user) AS closed    JOIN " +
      " (SELECT count(groupsProfiles.key) AS ClosedGroup, user    FROM groupsProfiles JOIN userGroupsSubs ON groupsProfiles.key = group WHERE is_closed = 1 GROUP BY user) AS notClosed ON  closed.user = notClosed.user")

  }

  def question6SQL(spark: SparkSession, userWallProfiles: String, friends: String, followers: String): sql.DataFrame = {
    spark.read.parquet(userWallProfiles).createOrReplaceTempView("userWallProfiles")
    spark.read.parquet(followers).createOrReplaceTempView("followers")
    spark.read.parquet(friends).createOrReplaceTempView("friends")

    spark.sql("(SELECT id FROM userWallProfiles WHERE deactivated = 'deleted')").createOrReplaceTempView("Deleted")

    spark.sql("SELECT Followers.profile AS UserID,(DeletedFollower + DeletedFriend) AS DeletedFriendsFollowers FROM " +
      " (SELECT profile, count(follower) AS DeletedFollower   FROM followers JOIN Deleted ON Deleted.id = followers.follower GROUP BY profile) AS Followers JOIN" +
      " (SELECT profile, count(follower) AS DeletedFriend     FROM friends   JOIN Deleted ON Deleted.id = friends.follower   GROUP BY profile) AS Friends " +
      " ON Followers.profile = Friends.profile")
  }

  /*
   limited by 100 to avoid outOfMemoryException

    */
  def question7SQL(spark: SparkSession, userWallComments: String, userWallLikes: String, followers: String, friends: String): sql.DataFrame = {

    spark.read.parquet(userWallComments).createOrReplaceTempView("userWallComments")
    spark.read.parquet(userWallLikes).createOrReplaceTempView("userWallLikes")
    spark.read.parquet(followers).createOrReplaceTempView("followers")
    spark.read.parquet(friends).createOrReplaceTempView("friends")

    spark.sql("SELECT count(from_id) AS NbCommentByFollowers,from_id AS FollowerID, post_owner_id AS UserID FROM userWallComments WHERE from_id IN (SELECT profile FROM followers) GROUP BY UserID,from_id LIMIT 1000")
      .createOrReplaceTempView("CommentOnWallByFollower")

    spark.sql("SELECT count(likerId) AS NbLikesByFollowers,likerId AS FollowerID, ownerId AS UserID FROM userWallLikes WHERE likerId IN (SELECT profile FROM followers) GROUP BY UserID,likerId LIMIT 1000")
      .createOrReplaceTempView("LikesOnWallByFollower")


    spark.sql("SELECT count(from_id) AS NbCommentByFriends,from_id  AS FriendID, post_owner_id AS UserID FROM userWallComments WHERE from_id IN (SELECT profile FROM friends) GROUP BY UserID,from_id LIMIT 1000")
      .createOrReplaceTempView("CommentOnWallByFriend")

    spark.sql("SELECT count(likerId) AS NbLikesByFriends,likerId AS FriendID, ownerId AS UserID FROM userWallLikes WHERE likerId IN (SELECT profile FROM friends) GROUP BY UserID,likerId LIMIT 1000")
      .createOrReplaceTempView("LikesOnWallByFriend")


    spark.sql("SELECT LikesOnWallByFriend.UserID," +
      " AVG(NbCommentByFollowers) AS AVGCommentByFollower,COUNT(NbCommentByFollowers) AS nbOfDiffFollowersCommenting,MAX(NbCommentByFollowers) AS maxNbOfCommentByFollower, " +
      " AVG(NbLikesByFollowers)   AS AVGLikeByFollower,   COUNT(NbLikesByFollowers)   AS nbOfDiffFollowersLiking,    MAX(NbLikesByFollowers)   AS maxNbOfLikeByFollower ," +
      " AVG(NbCommentByFriends)   AS AVGCommentByFriend,  COUNT(NbCommentByFriends)   AS nbOfDiffFriendCommenting,   MAX(NbCommentByFriends)   AS maxNbOfCommentByFriend," +
      " AVG(NbLikesByFriends)     AS AVGLikeByFriend,     COUNT(NbLikesByFriends)     AS nbOfDiffFriendLiking,       MAX(NbLikesByFriends)     AS maxNbOfLikeByFriend FROM" +
      " CommentOnWallByFollower JOIN " +
      " LikesOnWallByFollower   ON CommentOnWallByFollower.UserID = LikesOnWallByFollower.UserID JOIN " +
      " CommentOnWallByFriend   ON LikesOnWallByFollower.UserID = CommentOnWallByFriend.UserID JOIN " +
      " LikesOnWallByFriend     ON CommentOnWallByFriend.UserID = LikesOnWallByFriend.UserID" +
      " GROUP BY LikesOnWallByFriend.UserID")
  }

  def question9SQL(spark: SparkSession, strpath: String): sql.DataFrame = {
    val q1 = strpath + "Questions/Q1SQL.parquet" //UserID
    val q2 = strpath + "Questions/Q2SQL.parquet" //UserID
    val q3 = strpath + "Questions/Q3SQL.parquet" //PostID
    val q4 = strpath + "Questions/Q4SQL.parquet" //PostID
    val q5 = strpath + "Questions/Q5SQL.parquet" //UserID
    // val q6 = strpath + "Questions/Q6DF.parquet" //UserID

    val q1parquet = spark.read.parquet(q1).createOrReplaceTempView("Q1")

    val q2parquet = spark.read.parquet(q2).createOrReplaceTempView("Q2")

    val q3parquet = spark.read.parquet(q3).createOrReplaceTempView("Q3")

    val q4parquet = spark.read.parquet(q4).createOrReplaceTempView("Q4")

    val q5parquet = spark.read.parquet(q5).createOrReplaceTempView("Q5")

    // val q6parquet = spark.read.parquet(q6).as("Q6")

    spark.sql("SELECT Q1.UserID, Likes, Post, Comment, Follower, Friend, Group, Photo, Gif, NotClosedGroup, ClosedGroup" +
      " FROM Q1 JOIN Q2 ON Q1.UserID = Q2.UserID JOIN " +
      " Q5 ON Q5.UserID = Q1.UserID")

  }



  def question1DF(spark: SparkSession, userWallComments: String, userWallLikes: String, userWallPosts: String): sql.DataFrame = {
    import spark.implicits._ // allow to use $

    val parquetuserWallLikes = spark.read.parquet(userWallLikes).as("userWallLikes")
      .select($"itemId",$"likerId".as("UserIDL"))
      .groupBy($"UserIDL").agg(count("itemId").as("Likes"))

    val parquetuserWallPosts = spark.read.parquet(userWallPosts).as("userWallPosts")
      .select($"id", $"owner_id".as("UserIDP"))
      .groupBy($"UserIDP").agg(count("id").as("Posts"))

    val parquetuserWallComments = spark.read.parquet(userWallComments).as("userWallComments")
      .select($"post_id",$"post_owner_id".as("UserIDC"))
      .groupBy($"UserIDC").agg(count("post_id").as("Comment"))


    val postLike = parquetuserWallLikes.join(parquetuserWallPosts, parquetuserWallLikes("UserIDL") === parquetuserWallPosts("UserIDP"))
    postLike.join(parquetuserWallComments, parquetuserWallComments("UserIDC") === postLike("UserIDL"))
      .select($"UserIDL".as("UserID"), $"Likes",$"Posts",$"Comment")
  }

  def question2DF(spark: SparkSession, friends: String, followers: String, userGroupsSubs: String, userWallPhotos: String): sql.DataFrame = {
    import spark.implicits._ // allow to use $

    val parquetfollowers = spark.read.parquet(followers).as("followers")
      .select($"follower",$"profile".as("UserIDFO"))
      .groupBy($"UserIDFO").agg(count("follower").as("Follower"))

    val parquetfriends = spark.read.parquet(friends).as("friends")
      .select($"follower", $"profile".as("UserIDFR"))
      .groupBy($"UserIDFR").agg(count("follower").as("Friend"))

    val parquetuserGroupsSubs = spark.read.parquet(userGroupsSubs).as("userGroupsSubs")
      .select($"group",$"user".as("UserIDGR"))
      .groupBy($"UserIDGR").agg(count("group").as("Group"))

    val parquetuserWallPhotos = spark.read.parquet(userWallPhotos).as("userWallPhotos")
      .select($"photo_75",$"owner_id".as("UserIDPH"))
      .filter($"photo_75".like("%.jpg"))
      .groupBy($"UserIDPH").agg(count("photo_75").as("Photo"))

    val parquetuserWallGif = spark.read.parquet(userWallPhotos).as("userWallPhotos")
      .select($"photo_75",$"owner_id".as("UserIDGIF"))
      .filter($"photo_75".like("%.gif"))
      .groupBy($"UserIDGIF").agg(count("photo_75").as("Gif"))


    val parquetFollFri = parquetfollowers.join(parquetfriends, parquetfollowers("UserIDFO") === parquetfriends("UserIDFR"))
    val parquetFollFriGroup = parquetFollFri.join(parquetuserGroupsSubs,parquetFollFri("UserIDFO")===parquetuserGroupsSubs("UserIDGR"))
    val parquetFollFriGroupPhoto = parquetFollFriGroup.join(parquetuserWallPhotos, parquetFollFriGroup("UserIDGR") === parquetuserWallPhotos("UserIDPH"))
    parquetFollFriGroupPhoto.join(parquetuserWallGif, parquetFollFriGroupPhoto("UserIDPH") === parquetuserWallGif("UserIDGIF"))
      .select($"UserIDFO".as("UserID"), $"Follower",$"Friend",$"Group",$"Photo",$"Gif")
  }

  def question3DF(spark: SparkSession, userWallComments: String): sql.DataFrame = {
    import spark.implicits._ // allow to use $

    val parquetUserWallComments = spark.read.parquet(userWallComments).as("userWallComments")
      .select($"post_id".as("PostID"),$"from_id".as("UserID"))
      .filter($"from_id" =!= $"post_owner_id")

    val paquetcountPost = parquetUserWallComments.select($"PostID",$"UserID",$"PostID")
      .groupBy($"PostID",$"UserID").agg(count($"PostID").as("NbPost"))

    paquetcountPost.select($"PostID",$"NbPost")
      .groupBy($"PostID").agg(avg($"NbPost").as("AVGPost"),max($"NbPost").as("MAXPost"),sum($"NbPost").as ("NbOfPost"))
  }

  def question4DF(spark: SparkSession, userWallLikes: String): sql.DataFrame = {
    import spark.implicits._ // allow to use $

    val parquetUserLikes = spark.read.parquet(userWallLikes).as("userWallLikes")
      .select($"itemId".as("PostID"),$"likerId".as("UserID"))
      .filter($"likerId" =!= $"ownerId")

    val paquetcountPost = parquetUserLikes.select($"PostID",$"UserID",$"PostID")
      .groupBy($"PostID",$"UserID").agg(count($"PostID").as("NbPost"))

    paquetcountPost.select($"PostID",$"NbPost")
      .groupBy($"PostID").agg(avg($"NbPost").as("AVGPost"),max($"NbPost").as("MAXPost"),sum($"NbPost").as("NbOfPost"))
  }

  def question5DF(spark: SparkSession, groupsProfiles: String, userGroupsSubs : String): sql.DataFrame = {
    import spark.implicits._ // allow to use $

    val parquetgroupsProfiles = spark.read.parquet(groupsProfiles).as("groupsProfiles")
    val parquetuserGroupsSubs = spark.read.parquet(userGroupsSubs).as("userGroupsSubs")
    val parquetNotClosed = parquetuserGroupsSubs.join(parquetgroupsProfiles,parquetgroupsProfiles("key") === parquetuserGroupsSubs("group"))
      .select(parquetgroupsProfiles("key"),$"user")
      .filter($"is_closed" === 0)
      .groupBy($"user").agg(count($"key").as("NotClosedGroup"))

    val parquetgroupsProfiles2 = spark.read.parquet(groupsProfiles).as("groupsProfiles")
    val parquetuserGroupsSubs2 = spark.read.parquet(userGroupsSubs).as("userGroupsSubs")
    val parquetClosed = parquetuserGroupsSubs2.join(parquetgroupsProfiles2,parquetgroupsProfiles2("key") === parquetuserGroupsSubs2("group"))
      .select(parquetgroupsProfiles2("key"),$"user")
      .filter($"is_closed" === 1)
      .groupBy($"user").agg(count($"key").as("ClosedGroup"))

    parquetNotClosed.join(parquetClosed,parquetNotClosed("user") ===  parquetClosed("user"))
      .select(parquetClosed("user").as("UserID"),$"NotClosedGroup",$"ClosedGroup")
  }

  def question6DF(spark: SparkSession, userWallProfiles: String, friends: String, followers: String): sql.DataFrame = {
    import spark.implicits._ // allow to use $
    val parquetDeleted = spark.read.parquet(userWallProfiles).as("userWallProfiles")
      .select($"id")
      .filter($"deactivated" === "deleted")

    val parquetfollowers = spark.read.parquet(followers).as("followers")

    val parquetFriend = spark.read.parquet(friends).as("friends")


    val parquetfolloDel = parquetfollowers.join(parquetDeleted,parquetDeleted("id") === parquetfollowers("follower"))
      .select($"profile", $"follower")
      .groupBy($"profile").agg(count($"follower").as("DeletedFollower"))

    val parquetfriDel = parquetFriend.join(parquetDeleted,parquetDeleted("id") === parquetFriend("follower"))
      .select($"profile", $"follower")
      .groupBy($"profile").agg(count($"follower").as("DeletedFriend"))


    parquetfriDel.join(parquetfolloDel,parquetfriDel("profile") === parquetfolloDel("profile"))
      .select(parquetfolloDel("profile").as("UserID"),$"DeletedFriend",$"DeletedFollower")
  }

  /*
  limited by 1000 to avoid outOfMemoryException

   */
  def question7DF(spark: SparkSession, userWallComments: String, userWallLikes: String, followers: String, friends: String): sql.DataFrame = {
    import spark.implicits._ // allow to use $

    val parquetFollowers = spark.read.parquet(followers).as("followers").select($"profile")
    val parquetFriends = spark.read.parquet(followers).as("friends").select($"profile")


    val parquetCommentOnWallByFollowerO = spark.read.parquet(userWallComments).as("userWallComments")
    val parquetCommentOnWallByFollower = parquetCommentOnWallByFollowerO.join(parquetFollowers, parquetCommentOnWallByFollowerO("from_id") === parquetFollowers("profile"))
      .select($"from_id", $"from_id", $"post_owner_id".as("UserIDFoC"))
      .groupBy($"UserIDFoC", $"from_id").agg(count($"from_id").as("NbCommentByFollowers")).limit(1000)

    val parquetLikeOnWallByFollowerO = spark.read.parquet(userWallLikes).as("userWallLikes")
    val parquetLikeOnWallByFollower = parquetLikeOnWallByFollowerO.join(parquetFollowers, parquetLikeOnWallByFollowerO("likerId") === parquetFollowers("profile"))
      .select($"likerId", $"likerId", $"ownerId".as("UserIDFoL"))
      .groupBy($"UserIDFoL", $"likerId").agg(count($"likerId").as("NbLikesByFollowers")).limit(1000)

    val parquetCommentOnWallByFriendO = spark.read.parquet(userWallComments).as("userWallComments")
    val parquetCommentOnWallByFriend = parquetCommentOnWallByFriendO.join(parquetFriends, parquetCommentOnWallByFriendO("from_id") === parquetFriends("profile"))
      .select($"from_id", $"from_id", $"post_owner_id".as("UserIDFrC"))
      .groupBy($"UserIDFrC", $"from_id").agg(count($"from_id").as("NbCommentByFriends")).limit(1000)

    val parquetLikeOnWallByFriendO = spark.read.parquet(userWallLikes).as("userWallLikes")
    val parquetLikeOnWallByFriend = parquetLikeOnWallByFriendO.join(parquetFriends, parquetLikeOnWallByFriendO("likerId") === parquetFriends("profile"))
      .select($"likerId", $"likerId", $"ownerId".as("UserIDFrL"))
      .groupBy($"UserIDFrL", $"likerId").agg(count($"likerId").as("NbLikesByFriends")).limit(1000)

    val parquetAllfollowers = parquetCommentOnWallByFollower.join(parquetLikeOnWallByFollower, parquetCommentOnWallByFollower("UserIDFoC") === parquetLikeOnWallByFollower("UserIDFoL"))
    val parquetAllfriends = parquetCommentOnWallByFriend.join(parquetLikeOnWallByFriend, parquetCommentOnWallByFriend("UserIDFrC") === parquetLikeOnWallByFriend("UserIDFrL"))
    val allTables = parquetAllfollowers.join(parquetAllfriends, parquetAllfollowers("UserIDFoC") === parquetAllfriends("UserIDFrC"))

    allTables.select($"UserIDFrC".as("UserID"),
      $"NbCommentByFollowers", $"NbCommentByFollowers", $"NbCommentByFollowers",
      $"NbLikesByFollowers", $"NbLikesByFollowers", $"NbLikesByFollowers",
      $"NbCommentByFriends", $"NbCommentByFriends", $"NbCommentByFriends",
      $"NbLikesByFriends", $"NbLikesByFriends", $"NbLikesByFriends")
      .groupBy($"UserID").agg(
      avg($"NbCommentByFollowers").as("AVGCommentByFollower"), count($"NbCommentByFollowers").as("nbOfDiffFollowersCommenting"), max($"NbCommentByFollowers").as("maxNbOfCommentByFollower"),
      avg($"NbLikesByFollowers").as("AVGLikeByFollower"), count($"NbLikesByFollowers").as("nbOfDiffFollowersLiking"), max($"NbLikesByFollowers").as("maxNbOfLikeByFollower"),
      avg($"NbCommentByFriends").as("AVGCommentByFriend"), count($"NbCommentByFriends").as("nbOfDiffFriendCommenting"), max($"NbCommentByFriends").as("maxNbOfCommentByFriend"),
      avg($"NbLikesByFriends").as("AVGLikeByFriend"), count($"NbLikesByFriends").as("nbOfDiffFriendLiking"), max($"NbLikesByFriends").as("maxNbOfLikeByFriend")
    )
  }


  def question9DF(spark: SparkSession, strpath: String): sql.DataFrame = {
      import spark.implicits._ // allow to use $
      val q1 = strpath + "Questions/Q1DF.parquet" //UserID
      val q2 = strpath + "Questions/Q2DF.parquet" //UserID
      val q3 = strpath + "Questions/Q3DF.parquet" //PostID
      val q4 = strpath + "Questions/Q4DF.parquet" //PostID
      val q5 = strpath + "Questions/Q5DF.parquet" //UserID
      // val q6 = strpath + "Questions/Q6DF.parquet" //UserID

      val q1parquet = spark.read.parquet(q1).as("Q1")

      val q2parquet = spark.read.parquet(q2).as("Q2")

      val q3parquet = spark.read.parquet(q3).as("Q3")

      val q4parquet = spark.read.parquet(q4).as("Q4")

      val q5parquet = spark.read.parquet(q5).as("Q5")

      // val q6parquet = spark.read.parquet(q6).as("Q6")

      val q1q2 = q1parquet.join(q2parquet, q1parquet("UserID") === q2parquet("UserID")).select(q1parquet("UserID"), $"Likes", $"Posts", $"Comment", $"Follower", $"Friend", $"Group", $"Photo", $"Gif")
      q1q2.join(q5parquet, q1q2("UserID") === q5parquet("UserID")).select(q1q2("UserID"), $"Likes", $"Posts", $"Comment", $"Follower", $"Friend", $"Group", $"Photo", $"Gif", $"NotClosedGroup", $"ClosedGroup")
  }

  def question1API(spark: SparkSession,userWallComments:String, userWallLikes:String, userWallPosts:String): RDD[(Long, (Int, (Int, Int)))] = {
    val apiuserWallLikes = spark.read.parquet(userWallLikes).rdd;
    val apiuserWallLikesSelect = apiuserWallLikes.map(row => (row.getLong(row.fieldIndex("likerId")), row))
      .groupBy(likerId => likerId._1)
      .map { case (likerId, group) => (likerId, group.size) }

    val apiuserWallPosts = spark.read.parquet(userWallPosts).rdd;
    val apiuserWallPostsSelect = apiuserWallPosts.map(row => (row.getLong(row.fieldIndex("owner_id")), row))
      .groupBy(owner_id => owner_id._1)
      .map { case (owner_id, group) => (owner_id, group.size) }

    val apiuserWallComments = spark.read.parquet(userWallComments).rdd;
    val apiuserWallCommentsSelect = apiuserWallComments.map(row => (row.getLong(row.fieldIndex("post_owner_id")), row))
      .groupBy(post_owner_id => post_owner_id._1)
      .map { case (post_owner_id, group) => (post_owner_id, group.size) }

    apiuserWallLikesSelect.join(apiuserWallPostsSelect.join(apiuserWallCommentsSelect))
  }

}