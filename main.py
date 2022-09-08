import pandas as pd
from youtube_scrapper_test import YoutubeScrapper





# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    driver_loc = r"C:\Users\dnsingh\Downloads\Compressed\chromedriver_win32\chromedriver.exe"
    url = "https://www.youtube.com/c/HiteshChoudharydotcom/videos"
    n=49
    scrape = YoutubeScrapper(url,n,driver_loc)
    scrape.final_process()

#https://www.youtube.com/user/krishnaik06/videos
#https://www.youtube.com/user/saurabhexponent1/videos
#https://www.youtube.com/c/Telusko/videos
#https://www.youtube.com/c/HiteshChoudharydotcom/videos