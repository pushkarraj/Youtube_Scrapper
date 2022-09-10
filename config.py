from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
import logging
import os


class configuration():

    def __init__(self,driver_loc):
        self.driver_loc=driver_loc

    def driver_init(self):
        try:
            options = webdriver.ChromeOptions()
            options._binary_location = os.environ.get("GOOGLE_CHROME_BIN")
            options.add_argument("start-maximized")
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--incognito")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.page_load_strategy = 'eager'
            service = Service(executable_path=os.environ.get("CHROMEDRIVER_PATH"))
            driver = webdriver.Chrome(service=service, options=options)
            driver.delete_all_cookies()
            driver.set_page_load_timeout(20)
            driver.set_script_timeout(20)
            return driver
        except Exception as e:
            logging.error("Driver Initialization Failed")


    def logger(self):
            logging.basicConfig(filename="LOG.log", level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    def wait_init(self,d):
        try:
            return WebDriverWait(d, 20)
        except Exception as e:
            logging.error("Wait Initilization failed")
