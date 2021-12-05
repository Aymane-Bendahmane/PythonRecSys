from flask import Flask, json, jsonify
from flask import request
import numpy as np
import pandas as pd
import psycopg2
import flask_cors 
import py_eureka_client.eureka_client as eureka_client
from  flask_cors import CORS

#   How ro run the project
#   pip install pandas
#   pip install flask
#   pip install numpy
#   pip install psycopg2
#   pip install flask pandas py-eureka-client
#   $env:FLASK_APP = "pythonWebService"
#   python -m flask run
rest_port = 8050
eureka_client.init(eureka_server="http://localhost:8761/eureka",
                   app_name="Recommendation-system",
                   instance_host='localhost',
                   instance_port=rest_port)
pythonWebService = Flask(__name__)
CORS(pythonWebService)

@pythonWebService.route("/Rec", methods=["GET"])
def home():
    id = request.args.get('id', default=1, type=int)

    query_results = getDataFromDb()
    print(query_results)
    columns = []

    df = pd.DataFrame(query_results)
    df.columns = ["rating_id", "comment", "date",
                  "rating", "article_id", "id_user"]
    df.pop("comment")
    df.pop("date")
    print(df)
    df.groupby('article_id')['rating'].mean().sort_values(ascending=False)
    df.groupby('article_id')['rating'].count().sort_values(ascending=False)
    ratings = pd.DataFrame(df.groupby('article_id')['rating'].mean())
    # print(ratings)
    ratings['num of ratings'] = pd.DataFrame(
        df.groupby('article_id')['rating'].count())
    moviemat = df.pivot_table(
        index='id_user', columns='article_id', values='rating')

    ratings.sort_values('num of ratings', ascending=False)
    item0 = moviemat[id]

    similar_to_item0 = moviemat.corrwith(item0)

    corr_item0 = pd.DataFrame(similar_to_item0, columns=['Correlation'])
    corr_item0.dropna(inplace=True)
    corr_item0.sort_values('Correlation', ascending=False)
    print('-----------------------------------------------')
    print(corr_item0)
    corr_item0 = corr_item0.join(ratings['num of ratings'])
    recommendation = corr_item0[corr_item0['num of ratings'] > 3].sort_values(
        'Correlation', ascending=False)
    print('-----------------------------------------------')
    print(recommendation)

    return recommendation.head().to_dict()


def getDataFromDb():
    conn = psycopg2.connect(host="localhost", port=5432,
                            database="Ecommerce_db", user="postgres", password="admin")
    cur = conn.cursor()
    cur.execute("""SELECT * FROM rating""")
    query_results = cur.fetchall()

    return query_results


@pythonWebService.route("/db")
def db():
    conn = psycopg2.connect(host="localhost", port=5432,
                            database="Ecommerce_db", user="postgres", password="admin")
    cur = conn.cursor()
    cur.execute("SELECT * FROM rating")
    query_results = cur.fetchall()
    print(query_results)
    return jsonify({"Ratings": query_results})

if __name__ == "__main__":
    pythonWebService.run(host='localhost', port = rest_port)