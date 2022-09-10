from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from bs4 import BeautifulSoup as bs
import time
import os
import logging
from config import configuration
from database import snowflakes,mongo_db_datawriter,mongo_db_image_writer
import pandas as pd
import pymongo



class YoutubeScrapper(configuration):

    def __init__(self, url, n,driver_loc):
        self.url = url
        self.n = n
        self.ch_name=""
        self.nail = []
        self.title = []
        self.vlink = []
        self.normal_commenter = []
        self.comments = []
        self.len_comment=[]
        self.likes = []
        self.views = []
        configuration.__init__(self,driver_loc)

    def video_box(self, d):
        try:
            return d.find_elements(By.TAG_NAME, "ytd-grid-video-renderer")
        except Exception as e:
            logging.error("Video Box not found on the web page")

    def comment_box(self, s):
        try:
            return s.find_all("ytd-comment-thread-renderer")
        except Exception as e:
            logging.error("Comment Box not found on the web page")

    def video_link(self, e):
        try:
            return e.find_element(By.TAG_NAME, "a").get_attribute("href")
        except:
            logging.info("Likes not Found")

    def video_title(self, e):
        try:
            return e.find_element(By.ID, "video-title").get_attribute("title")
        except:
            logging.info("Title not Found")

    def thumbnail(self, e):
        try:
            return e.find_element(By.TAG_NAME, "img").get_attribute("src")
        except:
            logging.info("Thumbnail not Found")

    def like_scrapper(self, e):
        try:
            return e.find_all("yt-formatted-string",
                              {'class': "style-scope ytd-toggle-button-renderer style-text", 'id': "text"})[0][
                       "aria-label"][:-6].replace(",", "")
        except:
            logging.info("Likes could not be scrapped")

    def view_scrapper(self, e):
        try:
            return e.find_all("meta", itemprop="interactionCount")[0]["content"]
        except:
            logging.info("Views could not be scrapped")

    def commenter(self, s):
        try:
            return s.find("span").text.strip(" ,\n")
        except:
            logging.info("Name of Commentor could not be Scrapped")

    def comment_scrape(self, s):
        try:
            return s.find("yt-formatted-string", {"class": "style-scope ytd-comment-renderer"}, id="content-text").text
        except:
            logging.info("Comments Could not be Scrapped")

    def channel_name(self,e):
        try:
            return e.find_element(By.CLASS_NAME,"style-scope ytd-channel-name").text
        except:
            logging.info("Channel name could not be scrapped")

    def page1_scrape(self,driver,wait):
        try:
            driver.get(self.url)
            time.sleep(1)
            self.ch_name = self.channel_name(driver)
            print(self.ch_name)
            driver.execute_script("document.body.style.zoom = '0.25'")
            box = self.video_box(driver)
            while len(box) < self.n:
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
                box = self.video_box(driver)
                wait.until(EC.visibility_of_all_elements_located((By.TAG_NAME, "ytd-grid-renderer")))
            i = 0
            while i <= self.n:
                print(i)
                element = box[i]
                if "short" in self.video_link(element):
                    self.n += 1
                    i += 1
                    continue
                self.vlink.append(self.video_link(element))
                self.nail.append(self.thumbnail(element))
                self.title.append(self.video_title(element))
                i += 1
        except Exception as e:
            logging.error(e)

    def page2_scrape(self,driver,wait):
        try:
            for i in range(0, len(self.vlink)):
                print(i)
                driver.get(self.vlink[i])
                time.sleep(1)  # dynamic wating needed.
                driver.execute_script("document.body.style.zoom = '0.1'")
                wait.until(EC.visibility_of_any_elements_located((By.TAG_NAME, "ytd-comment-thread-renderer")))
                comment_box_old = driver.find_elements(By.TAG_NAME, "ytd-comment-thread-renderer")
                driver.execute_script("arguments[0].scrollIntoView();", comment_box_old[-1])

                while True:
                    print("Loading")
                    driver.execute_script("arguments[0].scrollIntoView();", comment_box_old[-1])
                    wait.until(EC.visibility_of_all_elements_located((By.TAG_NAME, "ytd-comment-thread-renderer")))
                    comment_box_new = driver.find_elements(By.TAG_NAME, "ytd-comment-thread-renderer")
                    if len(comment_box_old) == len(comment_box_new):
                        break
                    comment_box_old = comment_box_new

                soup = bs(driver.page_source, "html.parser")

                self.likes.append(self.like_scrapper(soup))
                self.views.append(self.view_scrapper(soup))
                c_box = self.comment_box(soup)

                channel_commenter = c_box[0].find_all("yt-formatted-string", {'class': "style-scope ytd-channel-name"})
                if len(channel_commenter) > 0:
                    c_box.pop(0)

                self.len_comment.append(len(c_box))

                for i in range(0, len(c_box)):
                    self.normal_commenter.append(self.commenter(c_box[i]))
                    self.comments.append(self.comment_scrape(c_box[i]))

                driver.switch_to.new_window("tab")
                #driver.quit()
        except Exception as e:
            logging.info(str(e))

    def file_db_manage(self):
        target_folder = os.path.join('./csv_files', '_'.join(self.ch_name.lower().split(' ')))
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

        df = pd.DataFrame({'title': self.title, 'thumbnail': self.nail, 'video_link': self.vlink, 'views': self.views,'likes': self.likes,'no_comments':self.len_comment})
        df.to_csv(target_folder+"/"+self.ch_name+"_Video_Detail.csv")
        snowflakes(df, 'video_detail',self.ch_name)  # not double quotes
        mongo_db_datawriter("Youtube",self.ch_name+'_video_detail',df)

        df1 = pd.DataFrame(zip(self.normal_commenter, self.comments), columns=['commentor', "comment"])
        df1.to_csv(target_folder+"/"+self.ch_name+"_Comment_Detail.csv")
        snowflakes(df1, 'comment_detail',self.ch_name)  # not double quotes
        mongo_db_datawriter("Youtube", self.ch_name+"_comment_detail", df1)

        mongo_db_image_writer("Youtube_thumbnails", self.ch_name, self.nail)

        print("Video,Thumbanail & Comment Details generated as CSV and uploaded on DB")

    def final_process(self):
        start_time=time.time()
        driver = self.driver_init()
        wait=self.wait_init(driver)
        self.logger()
        self.page1_scrape(driver,wait)
        self.page2_scrape(driver,wait)
        driver.quit()
        self.file_db_manage()
        time_taken=time.time()-start_time
        print(time_taken)

        return self.vlink, self.nail, self.title,self.likes,self.views,self.normal_commenter, self.comments,self.len_comment

