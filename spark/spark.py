'''
usage: spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 spark.py <db_name>
'''

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode
import sys


if __name__ == '__main__':

    db_name = sys.argv[1]
    # Create SparkSession
    print('Init spark.py...')
    spark = SparkSession.builder.appName(
        'MongoSpark'
        ).config(
            "spark.mongodb.input.uri", 
            "mongodb://localhost:27017/{}".format(db_name)
        ).config(
            "spark.mongodb.output.uri",
            "mongodb://localhost:27017/{}".format(db_name)
        ).getOrCreate()

    spark.sparkContext.getConf().getAll()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Load Movies
    moviesdf = spark.read.format(
        "com.mongodb.spark.sql.DefaultSource"
    ).option(
        "uri","mongodb://127.0.0.1:27017/{}.movies".format(db_name)
    ).load()
    # Print Schema DataFrame Movies
    print('Schema dataframe movie...')
    moviesdf.printSchema()
    # Show Movies loaded
    print('Dataframe movie loaded...')
    moviesdf.show(10)

    # Load Ratings
    ratingsdf = spark.read.format(
        "com.mongodb.spark.sql.DefaultSource"
    ).option(
        "uri","mongodb://127.0.0.1:27017/{}.ratings".format(db_name)
    ).load()
    # Print Schema Dataframe Ratings
    print('Schema dataframe rating...')
    ratingsdf.printSchema()
    # Print Ratings loaded
    print('Dataframe ratings loaded...')
    ratingsdf.show(10)

    # Train a collaborative filter model from existing ratings
    print('Create collaborative filter...')
    als = ALS(
        maxIter=10,regParam=0.5,
        userCol='userid', itemCol='movieid',
        ratingCol='rating', coldStartStrategy='drop'
    )
    
    train, test = ratingsdf.randomSplit([0.8, 0.2])
    alsmodel = als.fit(train)   

    # Predict Ratings(optional)
    print('Calculate predictions...')
    prediction = alsmodel.transform(test)
    #Recommeder movies - 5 movies per users
    print('Recommender 5 movies per users...')
    recommeder_movie = alsmodel.recommendForAllUsers(5)
    print('Print Schema recommender movie...')
    recommeder_movie.printSchema()
    '''
        recommender_movie
        |-- userid: integer (nullable = false)
        |-- recommendations: array (nullable = true)
        |    |-- element: struct (containsNull = true)
        |    |    |-- movieid: integer (nullable = true)
        |    |    |-- rating: float (nullable = true)
    '''
    print('Show 10 recommendation...')
    recommeder_movie.show(10, False)
    
    #Configure output to save mongoDB    
    print('Explode array recommendations...')
    explode_recom = recommeder_movie.select(
        recommeder_movie.userid,
        explode(recommeder_movie.recommendations)
    )
    print('Schema dataframe after exploding array...')
    explode_recom.printSchema()
    '''
        |-- userid: integer (nullable = false)
        |-- col: struct (nullable = true)
        |    |-- movieid: integer (nullable = true)
        |    |-- rating: float (nullable = true)    
    '''
    explode_recom.show()

    print('Explode struct data...')
    struct_explode = explode_recom.select('col.*', '*')
    struct_explode.printSchema()
    '''
        |-- movieid: integer (nullable = true)
        |-- rating: float (nullable = true)
        |-- userid: integer (nullable = false)
        |-- col: struct (nullable = true)
        |    |-- movieid: integer (nullable = true)
        |    |-- rating: float (nullable = true)
    '''
    print('Drop column struct_explode.col...')
    output = struct_explode.drop(struct_explode.col)
    print('Print schema output...')
    output.printSchema()
    print('Print dataframe output...')
    output.show(10)

    print('Make output with title movies')
    # Join Dataframes output and movie by column movieid
    print('Joining dataframes output and movie by movieid...')
    print('Drop [movies.movieid, moviesdf._id] in joined dataframe...')
    output_with_title = output.join(
        moviesdf,
        moviesdf.movieid == output.movieid
    ).drop(
        moviesdf.movieid
    ).drop(
        moviesdf._id
    )
    print('Print schema output with title...')
    output_with_title.printSchema()
    print('Print joined dataframes...')
    output_with_title.show(10)

    #Save mongoDB outputs
    print('Save output in mongoDB...')
    output.write.format(
        'com.mongodb.spark.sql.DefaultSource'
        ).mode(
            'append'
        ).option(
            'uri',"mongodb://127.0.0.1:27017/{}.recommendations".format(db_name)
        ).save()
    
    output_with_title.write.format(
        'com.mongodb.spark.sql.DefaultSource'
        ).mode(
            'append'
        ).option(
            'uri',"mongodb://127.0.0.1:27017/{}.recommendationstitle".format(db_name)
        ).save()

