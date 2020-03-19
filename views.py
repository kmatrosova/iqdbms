from flask import Flask, render_template, redirect, url_for
from flask_cors import CORS, cross_origin
import os
from flask import request
from flask_pymongo import PyMongo
from pymongo import MongoClient
import pprint

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/csv2tab"
mongo = PyMongo(app)
client = MongoClient()
db = client.csv2tab


CORS(app)


# Config options - Make sure you created a 'config.py' file.
app.config.from_object('config')
# To get one variable, tape app.config['MY_VARIABLE']

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/uploadcsv',methods=['GET','POST'])
def uploadcsv():
    print("In uploadcsv()")
    try:
        global a_file
        a_file = request.get_data(cache=False, as_text=True, parse_form_data=False)

        f = open("data.csv", "w")
        f.write(a_file)
        f.close()

    except Exception as exce:
        print(str(exce))
        return "True"
    return "oke"


@app.route('/SomeFunction')
def SomeFunction():
    my_file = "data.csv"
    print('Processing file...')
    os.system("python script.py")
    return "True"

@app.route('/results',methods=['GET','POST'])
def results():
    tables_names = mongo.db.list_collection_names()
    all_tables = {}
    for t in tables_names:
        if t != "ddre":
    #    all_tables[t] = (db[t].find())
    #print(all_tables)
            my_table = {}
            for i, r in enumerate(db[t].find()):
            #    pprint.pprint(r)
                r.pop('_id')
                my_table["r"+str(i)] = r
            all_tables[t] = my_table
    return render_template("results.html", all_tables=all_tables)

if __name__ == "__main__":
        app.run()
