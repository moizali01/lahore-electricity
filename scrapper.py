import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
import os
import time
import threading
import pandas as pd
# import gspread
# from gspread_dataframe import set_with_dataframe

def extractor(start_thread_value, end_thread_value,df, thread_id):
    # os.environ['PATH'] += ":/usr/lib/chromium-browser/chromedriver"
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(
        options=options
                            )
    for i in range(start_thread_value , end_thread_value):
        try:
            # driver = webdriver.Chrome()
            driver.get("http://www.lesco.gov.pk/Modules/CustomerBill/CheckBill.asp")
            ID_box = driver.find_element(By.XPATH,'/html/body/table/tbody/tr[5]/td/table/tbody/tr/td[2]/div/div[2]/form[2]/center/div/table/tbody/tr[2]/td[2]/input')
            ID_box.send_keys(f"{i}")
            find_bill = driver.find_element(By.XPATH, '/html/body/table/tbody/tr[5]/td/table/tbody/tr/td[2]/div/div[2]/form[2]/center/div/table/tbody/tr[4]/td/p/input[1]')
            driver.execute_script("arguments[0].click();", find_bill)

            view_full_bill = driver.find_element(By.XPATH, '/html/body/table/tbody/tr[5]/td/table/tbody/tr/td[2]/div/div[2]/form/font/button')
            driver.execute_script("arguments[0].click();", view_full_bill)

            address = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[5]')


            data = {}
            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[86]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[88]').get_attribute('innerHTML').strip()
            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[95]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[96]').get_attribute('innerHTML').strip()

            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[100]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[101]').get_attribute('innerHTML').strip()
            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[105]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[106]').get_attribute('innerHTML').strip()

            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[110]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[111]').get_attribute('innerHTML').strip()
            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[115]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[116]').get_attribute('innerHTML').strip()

            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[120]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[121]').get_attribute('innerHTML').strip()
            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[125]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[126]').get_attribute('innerHTML').strip()

            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[130]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[131]').get_attribute('innerHTML').strip()
            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[135]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[136]').get_attribute('innerHTML').strip()

            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[140]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[141]').get_attribute('innerHTML').strip()
            data[driver.find_element(By.XPATH, '/html/body/form/div[4]/p[145]').get_attribute('innerHTML').strip()] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[146]').get_attribute('innerHTML').strip()
        
            data['Feeder'] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[22]').get_attribute('innerHTML').strip()
            data['Sub-Division'] =  driver.find_element(By.XPATH, '/html/body/form/div[4]/p[24]').get_attribute('innerHTML').strip()
            data['Division'] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[26]').get_attribute('innerHTML').strip()
            
            data["JUN-23"] = (driver.find_element(By.XPATH, '/html/body/form/div[4]/p[168]').get_attribute('innerHTML').strip())[3:9]
            data["Last_year_change"] = (driver.find_element(By.XPATH, '/html/body/form/div[4]/p[168]').get_attribute('innerHTML').strip())[23:-4]
            data["Last_year_month"] = driver.find_element(By.XPATH, '/html/body/form/div[4]/p[169]').get_attribute('innerHTML').strip()[3:-4]
            data['Address'] = address.get_attribute('innerHTML').strip()
            data['Customer ID'] = i
        
            print(data)

            pd_df = pd.DataFrame.from_dict(data, orient='index').T
            df = pd.concat([df,pd_df], ignore_index=True)
            # write data of every 50 datapoints to csv for backup
            if len(df) % 50 == 0:
                file_name = "lesco-" + str(thread_id) + ".csv"
                # print("CSV MADE:", file_name)

                df.to_csv(file_name, index=False)

            print("Data User ID:",i)

        except Exception as e:
           print("User ID error:", i)
           #print("user ID error:", i, " : ", e)

    data_frames.append(df.copy())




if __name__ == '__main__':
    print('started')
    start_1=8500000
    ids_to_add_per_thread= 400
    number_threads= 250
    start_time = time.time()
    
    # add_custids(start_1,ids_to_add_per_thread*number_threads)

    data_frames = []
    df = pd.DataFrame()

    threads=[]

    for i in range(number_threads):
        t=threading.Thread(target=extractor,args=(start_1, start_1+ids_to_add_per_thread,df, i))
        t.start()
        threads.append(t)
        start_1=start_1+ids_to_add_per_thread
    for thread in threads:
        thread.join()

    end_time = time.time()

    print("Total Time taken:", end_time-start_time)

    # print("data_frames:",data_frames)
    combined_df = pd.concat(data_frames, ignore_index=True)
     # Print the resulting dataframe
    print("Combined DF\n",combined_df)
    # csv_file = '/Users/Moiz/Documents/lesco.csv'
    combined_df.to_csv('lesco.csv', index=False)