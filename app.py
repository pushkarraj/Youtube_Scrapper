import logging

from youtube_scrapper_test import YoutubeScrapper
from flask import Flask, render_template, request,jsonify
import pandas as pd


app = Flask(__name__)  # initialising the flask


@app.route('/', methods=['GET', 'POST']) # To render Homepage
def home_page():
    return render_template('index.html')


@app.route('/scrap', methods=['GET','POST']) # route with allowed methods as POST and GET
def index():
    if request.method == 'POST':
        try:
            driver_loc = r"C:\Users\dnsingh\Downloads\Compressed\chromedriver_win32\chromedriver.exe"
            n = 49
            scraper = YoutubeScrapper(url=request.form['content'],n=n,driver_loc=driver_loc)
            scraper.final_process()
            df = pd.DataFrame({'title': scraper.title, 'thumbnail': scraper.nail, 'video_link': scraper.vlink, 'views': scraper.views,
             'likes': scraper.likes,'no_comments':scraper.len_comment})
            df1 = pd.DataFrame(zip(scraper.normal_commenter, scraper.comments), columns=['commentor', "comment"])
            # result=scraper.title
            print('Result:-')
            print(df,request.form['content'])
            return render_template('table.html',tables=[df.to_html(),df1.to_html()], titles=["","Video_Details","Comment_Details"])
        except Exception as e:
            logging.error(str(e))
            return 'something is wrong'


if __name__ == "__main__":
    app.run()