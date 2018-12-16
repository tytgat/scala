import org.apache.spark.sql
import org.apache.spark.sql.functions.{avg, count, max, sum}
import org.apache.spark.sql.{SaveMode, SparkSession}


object DataFrameApp {
  def main(args: Array[String]): Unit = {
     val strpath = "/data/bgdata_small/";


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

    val q1SQLTimeS = System.nanoTime();  val q1SQL = question1SQL(spark, userWallComments, userWallLikes, userWallPosts); q1SQL.show();      val q1SQLTimeE = System.nanoTime(); val q1SQLExeTime = ((q1SQLTimeE-q1SQLTimeS) / 1e9d)+"sec"
    val q2SQLTimeS = System.nanoTime();  val q2SQL = question2SQL(spark, friends, followers, userGroupsSubs, userWallPhotos); q2SQL.show();  val q2SQLTimeE = System.nanoTime(); val q2SQLExeTime = ((q2SQLTimeE-q2SQLTimeS) / 1e9d)+"sec"
    val q3SQLTimeS = System.nanoTime();  val q3SQL = question3SQL(spark, userWallComments); q3SQL.show();                                    val q3SQLTimeE = System.nanoTime(); val q3SQLExeTime = ((q3SQLTimeE-q3SQLTimeS) / 1e9d)+"sec"
    val q4SQLTimeS = System.nanoTime();  val q4SQL = question4SQL(spark, userWallLikes); q4SQL.show();                                       val q4SQLTimeE = System.nanoTime(); val q4SQLExeTime = ((q4SQLTimeE-q4SQLTimeS) / 1e9d)+"sec"
    val q5SQLTimeS = System.nanoTime();  val q5SQL = question5SQL(spark, groupsProfiles, userGroupsSubs); q5SQL.show();                      val q5SQLTimeE = System.nanoTime(); val q5SQLExeTime = ((q5SQLTimeE-q5SQLTimeS) / 1e9d)+"sec"
    val q6SQLTimeS = System.nanoTime();  val q6SQL = question6SQL(spark, userWallProfiles, friends, followers); q6SQL.show();                val q6SQLTimeE = System.nanoTime(); val q6SQLExeTime = ((q6SQLTimeE-q6SQLTimeS) / 1e9d)+"sec"
    val q7SQLTimeS = System.nanoTime();  val q7SQL = question7SQL(spark, userWallComments, userWallLikes, followers, friends); q7SQL.show(); val q7SQLTimeE = System.nanoTime(); val q7SQLExeTime = ((q7SQLTimeE-q7SQLTimeS) / 1e9d)+"sec"



    val q1DFTimeS = System.nanoTime();  val q1DF = question1DF(spark, userWallComments, userWallLikes, userWallPosts); q1DF.show();      val q1DFTimeE = System.nanoTime(); val q1DFExeTime = ((q1DFTimeE-q1DFTimeS) / 1e9d)+"sec"
    val q2DFTimeS = System.nanoTime();  val q2DF = question2DF(spark, friends, followers, userGroupsSubs, userWallPhotos); q2DF.show();  val q2DFTimeE = System.nanoTime(); val q2DFExeTime = ((q2DFTimeE-q2DFTimeS) / 1e9d)+"sec"
    val q3DFTimeS = System.nanoTime();  val q3DF = question3DF(spark, userWallComments); q3DF.show();                                    val q3DFTimeE = System.nanoTime(); val q3DFExeTime = ((q3DFTimeE-q3DFTimeS) / 1e9d)+"sec"
    val q4DFTimeS = System.nanoTime();  val q4DF = question4DF(spark, userWallLikes); q4DF.show();                                       val q4DFTimeE = System.nanoTime(); val q4DFExeTime = ((q4DFTimeE-q4DFTimeS) / 1e9d)+"sec"
    val q5DFTimeS = System.nanoTime();  val q5DF = question5DF(spark, groupsProfiles, userGroupsSubs); q5DF.show();                      val q5DFTimeE = System.nanoTime(); val q5DFExeTime = ((q5DFTimeE-q5DFTimeS) / 1e9d)+"sec"
    val q6DFTimeS = System.nanoTime();  val q6DF = question6DF(spark, userWallProfiles, friends, followers); q6DF.show();                val q6DFTimeE = System.nanoTime(); val q6DFExeTime = ((q6DFTimeE-q6DFTimeS) / 1e9d)+"sec"
    val q7DFTimeS = System.nanoTime();  val q7DF = question7DF(spark, userWallComments, userWallLikes, followers, friends); q7DF.show(); val q7DFTimeE = System.nanoTime(); val q7DFExeTime = ((q7DFTimeE-q7DFTimeS) / 1e9d)+"sec"

    val q9DF = question9DF(q1DF,q2DF,q3DF,q4DF,q5DF,q6DF,q7DF);
    q9DF.show()

    println("|task\t| spark native api time \t| spark dataframe api \t| spark sql api \t| best \t|")
    println("|1\t| " + "|\t" + q1DFExeTime + "\t| " + q1SQLExeTime + "\t|" + bestTime(q1SQLTimeE-q1SQLTimeS,q1DFTimeE-q1DFTimeS,123456789) + "\t|" );
    println("|1\t| " + "|\t" + q2DFExeTime + "\t| " + q2SQLExeTime + "\t|" + bestTime(q2SQLTimeE-q2SQLTimeS,q2DFTimeE-q2DFTimeS,123456789) + "\t|" );
    println("|1\t| " + "|\t" + q3DFExeTime + "\t| " + q3SQLExeTime + "\t|" + bestTime(q3SQLTimeE-q3SQLTimeS,q3DFTimeE-q3DFTimeS,123456789) + "\t|" );
    println("|1\t| " + "|\t" + q4DFExeTime + "\t| " + q4SQLExeTime + "\t|" + bestTime(q4SQLTimeE-q4SQLTimeS,q4DFTimeE-q4DFTimeS,123456789) + "\t|" );
    println("|1\t| " + "|\t" + q5DFExeTime + "\t| " + q5SQLExeTime + "\t|" + bestTime(q5SQLTimeE-q5SQLTimeS,q5DFTimeE-q5DFTimeS,123456789) + "\t|" );
    println("|1\t| " + "|\t" + q6DFExeTime + "\t| " + q6SQLExeTime + "\t|" + bestTime(q6SQLTimeE-q6SQLTimeS,q6DFTimeE-q6DFTimeS,123456789) + "\t|" );
    println("|1\t| " + "|\t" + q7DFExeTime + "\t| " + q7SQLExeTime + "\t|" + bestTime(q7SQLTimeE-q7SQLTimeS,q7DFTimeE-q7DFTimeS,123456789) + "\t|" );

    //18/12/16 17:11:24 ERROR BroadcastExchangeExec: Could not execute broadcast in 300 secs.
    //Crash on question 7



    /*
     users
    .filter(
      _.id in persons
                .filter(p => p.name === "John" && p.surname === "Smith")
                .map(_.userId)
    )
    .map(_.email)
    .result

    select email
    from Users
    where id in (select userId
             from Persons
             where name = 'John'
             and surname = 'Smith')
     */
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
  limited by 100 to avoid outOfMemoryException

   */
  def question7DF(spark: SparkSession, userWallComments: String, userWallLikes: String, followers: String, friends: String): sql.DataFrame = {
    import spark.implicits._ // allow to use $

    val parquetFollowers = spark.read.parquet(followers).as("followers").select($"profile")
    val parquetFriends = spark.read.parquet(followers).as("friends").select($"profile")


    val parquetCommentOnWallByFollowerO = spark.read.parquet(userWallComments).as("userWallComments")
    val parquetCommentOnWallByFollower = parquetCommentOnWallByFollowerO.join(parquetFollowers, parquetCommentOnWallByFollowerO("from_id") === parquetFollowers("profile"))
      .select($"from_id",$"from_id",$"post_owner_id".as("UserIDFoC"))
      .groupBy($"UserIDFoC",$"from_id").agg(count($"from_id").as("NbCommentByFollowers")).limit(1000)

    val parquetLikeOnWallByFollowerO = spark.read.parquet(userWallLikes).as("userWallLikes")
    val parquetLikeOnWallByFollower = parquetLikeOnWallByFollowerO.join(parquetFollowers, parquetLikeOnWallByFollowerO("likerId") === parquetFollowers("profile"))
      .select($"likerId",$"likerId",$"ownerId".as("UserIDFoL"))
      .groupBy($"UserIDFoL",$"likerId").agg(count($"likerId").as("NbLikesByFollowers")).limit(1000)

    val parquetCommentOnWallByFriendO = spark.read.parquet(userWallComments).as("userWallComments")
    val parquetCommentOnWallByFriend = parquetCommentOnWallByFriendO.join(parquetFriends, parquetCommentOnWallByFriendO("from_id") === parquetFriends("profile"))
      .select($"from_id",$"from_id",$"post_owner_id".as("UserIDFrC"))
      .groupBy($"UserIDFrC",$"from_id").agg(count($"from_id").as("NbCommentByFriends")).limit(1000)

    val parquetLikeOnWallByFriendO = spark.read.parquet(userWallLikes).as("userWallLikes")
    val parquetLikeOnWallByFriend = parquetLikeOnWallByFriendO.join(parquetFriends, parquetLikeOnWallByFriendO("likerId") === parquetFriends("profile"))
      .select($"likerId",$"likerId",$"ownerId".as("UserIDFrL"))
      .groupBy($"UserIDFrL",$"likerId").agg(count($"likerId").as("NbLikesByFriends")).limit(1000)

    val parquetAllfollowers = parquetCommentOnWallByFollower.join(parquetLikeOnWallByFollower,parquetCommentOnWallByFollower("UserIDFoC") === parquetLikeOnWallByFollower("UserIDFoL"))
    val parquetAllfriends = parquetCommentOnWallByFriend.join(parquetLikeOnWallByFriend,parquetCommentOnWallByFriend("UserIDFrC") === parquetLikeOnWallByFriend("UserIDFrL"))
    val allTables = parquetAllfollowers.join(parquetAllfriends,parquetAllfollowers("UserIDFoC") ===  parquetAllfriends("UserIDFrC"))

    allTables.select($"UserIDFrC".as("UserID"),
      $"NbCommentByFollowers",$"NbCommentByFollowers",$"NbCommentByFollowers",
      $"NbLikesByFollowers",$"NbLikesByFollowers",$"NbLikesByFollowers",
      $"NbCommentByFriends",$"NbCommentByFriends",$"NbCommentByFriends",
      $"NbLikesByFriends",$"NbLikesByFriends",$"NbLikesByFriends")
      .groupBy($"UserID").agg(
      avg($"NbCommentByFollowers").as("AVGCommentByFollower"), count($"NbCommentByFollowers").as("nbOfDiffFollowersCommenting"), max($"NbCommentByFollowers").as("maxNbOfCommentByFollower"),
      avg($"NbLikesByFollowers").as("AVGLikeByFollower"),      count($"NbLikesByFollowers").as("nbOfDiffFollowersLiking"),       max($"NbLikesByFollowers").as("maxNbOfLikeByFollower"),
      avg($"NbCommentByFriends").as("AVGCommentByFriend"),     count($"NbCommentByFriends").as("nbOfDiffFriendCommenting"),      max($"NbCommentByFriends").as("maxNbOfCommentByFriend"),
      avg($"NbLikesByFriends").as("AVGLikeByFriend"),          count($"NbLikesByFriends").as("nbOfDiffFriendLiking"),            max($"NbLikesByFriends").as("maxNbOfLikeByFriend")
    )



  }
  def question9DF(q1: sql.DataFrame,q2: sql.DataFrame,q3: sql.DataFrame,q4: sql.DataFrame,q5: sql.DataFrame,q6: sql.DataFrame,q7: sql.DataFrame): sql.DataFrame = {

    q1.join(q2,q1("UserID") === q2("UserID"))
      //.join(q3,q1("UserID")===q3("UserID"))
      //.join(q4,q1("UserID")===q4("UserID"))
      .join(q5,q1("UserID")===q5("UserID"))
      .join(q6,q1("UserID")===q6("UserID"))
      .join(q7,q1("UserID")===q7("UserID"))
      .select("*")

  }
}