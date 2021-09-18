from flask import Flask, json,jsonify
from flask import request
import numpy as np
import pandas as pd
import psycopg2

pythonWebService = Flask(__name__)
@pythonWebService.route("/Rec" )
def home():
    id = request.args.get('id', default = 1, type = int)
    print(id)
    conn = psycopg2.connect(host="localhost", port = 5432, database="Ecommerce_db", user="postgres", password="admin")
    cur = conn.cursor()
    cur.execute("""SELECT * FROM rating""")
    query_results = cur.fetchall()

    df = pd.DataFrame(query_results,columns={"rating_id","rating","article_id","id_user"})

    df.groupby('article_id')['rating'].mean().sort_values(ascending=False)
    df.groupby('article_id')['rating'].count().sort_values(ascending=False)
    ratings = pd.DataFrame(df.groupby('article_id')['rating'].mean())
    ratings['num of ratings'] = pd.DataFrame(df.groupby('article_id')['rating'].count())
    moviemat = df.pivot_table(index='id_user',columns='article_id',values='rating')

    ratings.sort_values('num of ratings',ascending=False) 
    item0 = moviemat[id]


    similar_to_item0 = moviemat.corrwith(item0)

    corr_item0 = pd.DataFrame(similar_to_item0,columns=['Correlation'])
    corr_item0.dropna(inplace=True)
    corr_item0.sort_values('Correlation',ascending=False)
    corr_item0 = corr_item0.join(ratings['num of ratings'])
    recommendation = corr_item0[corr_item0['num of ratings']>0].sort_values('Correlation',ascending=False)
    return recommendation.head().to_dict()



@pythonWebService.route("/db")
def db():
    conn = psycopg2.connect(host="localhost", port = 5432, database="Ecommerce_db", user="postgres", password="admin")
    cur = conn.cursor()
    cur.execute("""SELECT * FROM rating""")
    query_results = cur.fetchall()
    print(query_results)
    return jsonify({"Ratings":query_results})

