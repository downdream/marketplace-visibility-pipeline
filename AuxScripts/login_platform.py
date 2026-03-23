# -*- coding: utf-8 -*-

### Python libraries ###
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

### Python scripts ###
from credentials import credentials_dict


'''Login on website'''
def login(mp, driver):
    username = credentials_dict[mp]['username']
    password = credentials_dict[mp]['password']
    
    ### Check24 ###
    if mp == 'check24':
        type_username = driver.find_element(By.XPATH,"//input[@id='login_email']")
        type_username.send_keys(username)
        time.sleep(2)
        type_password = driver.find_element(By.XPATH,"//input[@id='login_password']")
        type_password.send_keys(password)
        time.sleep(2)
        type_password.send_keys(Keys.RETURN)
        time.sleep(5)
    
    ### Metro ###
    if mp == 'metro':
        type_username = driver.find_element(By.NAME,"email")
        type_username.send_keys(username)
        time.sleep(2)
        type_password = driver.find_element(By.NAME,"password")
        type_password.send_keys(password)
        time.sleep(2)
        type_password.send_keys(Keys.RETURN)
        time.sleep(5)

    ### Check24 ###
    if mp == 'shopify':
        type_username = driver.find_element(By.XPATH,"//input[@id='account_email']")
        type_username.send_keys(username)
        type_password.send_keys(Keys.RETURN)
        time.sleep(2)
        type_password = driver.find_element(By.XPATH,"//input[@id='account_password']")
        type_password.send_keys(password)
        time.sleep(2)
        type_password.send_keys(Keys.RETURN)
        TOTP = input('totp?')