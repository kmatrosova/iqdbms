from flask import Flask, render_template, redirect, url_for
from flask_cors import CORS, cross_origin
import os
from flask import request


app = Flask(__name__)
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

@app.route('/results')
def results():
    return render_template('results.html')

if __name__ == "__main__":
        app.run()
