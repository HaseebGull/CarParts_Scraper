import csv
import datetime
import os
import traceback
import pyodbc
import time
from queue import Queue
from threading import Thread
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from scrapy.http import HtmlResponse
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from PIL import Image
from urllib.request import urlopen
from selenium.webdriver.common.keys import Keys


def web_driver():
    options = Options()
    options.add_experimental_option('excludeSwitches', ['ignore-certificate-errors'])
    options.add_argument('--disable-gpu')
    options.add_argument('headless')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    user_agent = 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
    options.add_argument('user-agent=' + user_agent + '')
    options.add_argument('--incognito')
    options.add_argument('disable-infobars')
    options.add_argument('--disable-browser-side-navigation')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    # driver.get(url)
    return driver


def driver_resp(r_url):
    p_driver = web_driver()
    p_driver.get(r_url)
    p_source = HtmlResponse(url=r_url, body=p_driver.page_source, encoding='utf-8')
    return p_source


def down_img(image_url):
    img_name = str(image_url).split('/')[-1]
    img = Image.open(urlopen(image_url))
    img.save(f"{'images'}/{img_name}")
    img_path = "images/" + img_name
    return img_path


def add_to_db(parts_list):
    print('Adding to db')
    write_log_file("Adding to db")
    for data in parts_list:
        cursor.execute(
            """INSERT INTO parts_data(`part_name`, `car_name`, `img_path`, `description`, `part_grade`,
             `stock_num`, `price`, `dealer_info`, `dist_mile`, `year`,`DatePriceUpdated`,
             `Description_sqlUser_inv`,`Interchange_Sql_inventory`,`Manufacturer`,`Model`,
             `Part`,`part_name_sqlUser_inv`,`part_type`)
              values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?)""",
            (data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[0],
             datetime.date.today(), des, interchange, input_data[2], input_data[3],
             part, part_input, part_type))
        conn.commit()


def get_parts_data(page_resp, page):
    write_log_file("Getting Relevant Data" + " page = " + str(page))
    global img_path
    parts_list = []
    tab_rows = page_resp.xpath('//table[@cellpadding="4" and @border="1"]/tbody/tr[not(@align)]')
    for tab_row in tab_rows:
        year = tab_row.xpath('./td[1]/text()[1]').get().strip()
        part_name = tab_row.xpath('./td[1]/text()[2]').get()
        car_name = tab_row.xpath('./td[1]/text()[3]').get().strip()
        if tab_row.xpath('.//img[@hspace="3"]'):
            image_url = tab_row.xpath('.//img[@hspace="3"]/@src').get()
            img_path = down_img(image_url)
        else:
            img_path = ''
        description = tab_row.xpath('./td[2]/text()').get().strip()
        part_grade = tab_row.xpath('./td[3]/text()').get().strip()
        stock_num = tab_row.xpath('./td[4]/text()').get().strip()
        price = tab_row.xpath('./td[5]/text()').get().strip()
        dealer_info = ' '.join(
            tab_row.xpath('.//td[6][not(contains(text(),"Dealer"))]/a[1]/text() | ./td[6]/text()').extract())
        dist_mile = tab_row.xpath('./td[7]/text()').get().strip()
        parts_list.append(
            [year, part_name, car_name, img_path, description, part_grade, stock_num, price, dealer_info, dist_mile])
    add_to_db(parts_list)
    try:
        page += 1
        next_page = page_resp.xpath("//span/a[contains(text()," + str(page) + ")]/@href").get()
        if not next_page.startswith(url):
            next_page = url + next_page
        next_page_resp = driver_resp(next_page)
        print("Next_page_data")
        get_parts_data(next_page_resp, page)
    except:
        pass


def write_err(ex, car_name, part_name):
    exx = traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)
    e_path = 'Error_Details' + str(datetime.datetime.now().strftime("%d-%m-%Y_%H-%M")) + '.csv'
    with open(e_path, 'a+', encoding='UTF8', newline='') as f:
        e_wr = csv.writer(f)
        if os.stat(e_path).st_size == 0:
            e_wr.writerow(['car_name', 'Part_name', 'StackTrace'])
        e_wr.writerow([car_name, part_name, exx])


def get_next_page(driver, car_name, part_name, start_year):
    driver.find_element(By.XPATH, '//select[@name="userDate"]').click()
    driver.find_element(By.XPATH, '//select[@name="userDate"]/option[@value="' + str(start_year) + '"]').click()
    driver.find_element(By.XPATH, '//select[@name="userModel"]/option[@value="' + str(car_name) + '"]').click()  # 1557
    driver.find_element(By.XPATH, '//select[@name="userPart"]/option[@value="' + str(part_name) + '"]').click()  # 658
    driver.find_element(By.XPATH, '//select[@name="userLocation"]//option[@value="USA"]').click()
    driver.find_element(By.XPATH, '//select[@name="userPreference"]//option[@value="price"]').click()
    driver.find_element(By.XPATH, '//input[@name="userZip"]').send_keys(Keys.CONTROL + "a")
    driver.find_element(By.XPATH, '//input[@name="userZip"]').send_keys('44622')
    driver.find_element(By.XPATH, '//input[@name="Search Car Part Inventory"]').click()


# car_name, part_name, start_year, id, des
def search(driver, q):
    while not q.empty():
        data = q.get(block=True)
        car_name = data[0]
        part_input = data[1]
        start_year = data[2]
        id = data[3]
        des = data[4]
        page = 1
        write_log_file(
            "Searching Data" + " " + str(car_name) + " " + str(part_input) + " " + str(start_year) + " " + str(des))
        driver.get(url)
        try:
            get_next_page(driver, car_name, part_input, start_year)
            time.sleep(1)
        except Exception as ex:
            exx = traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)
            print(exx)
            # write_err(e, car_name, part_name)
        p_resp = HtmlResponse(url=driver.current_url, body=driver.page_source, encoding='utf-8')
        # if we have options so we enter description from inputs
        try:
            if p_resp.xpath('//td[@nowrap]/input[not(contains(@value,"None")) and @name="dummyVar"]'):
                if p_resp.xpath('//td[@nowrap]/label[contains(text(),"' + str(des) + '")]'):
                    driver.find_element(By.XPATH, '//td[@nowrap]/label[contains(text(),"' + str(des) + '")]').click()
                    driver.find_element(By.XPATH, '//input[@name="Search Car Part Inventory"]').click()
                else:
                    pass
            elif p_resp.xpath('//input[@name="dbModel"]/following-sibling::label[contains(text(),"only")]'):
                driver.find_element(By.XPATH,
                                    '//input[@name="dbModel"]/following-sibling::label[contains(text(),"only")]').click()
                driver.find_element(By.XPATH, '//input[@name="Search Car Part Inventory"]').click()
            else:
                pass
            time.sleep(1)
            page_resp = HtmlResponse(url=driver.current_url, body=driver.page_source, encoding='utf-8')
            get_parts_data(page_resp, page)
            write_log_file(
                "Scraping Completed Of" + " " + str(car_name) + " " + str(part_input) + " " + str(
                    start_year) + " " + str(des))
        except Exception as e:
            write_err(e, car_name, part_name)
        try:
            q.task_done()
        except ValueError:
            pass

    print("tasks are completed")
    try:
        driver.quit()
    except:
        pass
    try:
        q.task_done()
    except ValueError:
        pass


def get_inputs(make_model, PartName, parts, models):
    global car_name, part_name
    for i in models:
        if i[0] == make_model:
            car_name = i[1]
    for j in parts:
        if j[0] == PartName:
            part_name = j[1]
    return (car_name, part_name)


def write_log_file(msg):
    f = open("log.txt", "a+")
    date_time = str(datetime.datetime.now().strftime("%d-%m-%Y_%H-%M"))
    f.write(date_time + " " + msg + " \n")
    f.close()


if __name__ == '__main__':
    list_1 = []
    write_log_file("Application Has Started")
    db_path = 'DB\\CarParts.accdb'
    conn = pyodbc.connect(
        "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" + rf'DBQ={db_path};')
    cursor = conn.cursor()
    url = 'https://www.car-part.com/'
    write_log_file("Getting Inputs from DB")
    manufacturer = cursor.execute(
        """SELECT ID1,Description,Manufacturer,Model,PartName,Yr, PartType,Part,Interchange FROM sql_inventory""")
    sql_inv_list = manufacturer.fetchall()
    parts_input = cursor.execute("""SELECT check_name, part_name FROM part_input""")
    parts = parts_input.fetchall()
    models_node = cursor.execute("""SELECT man_model, make_model FROM models""")
    models = models_node.fetchall()
    que = Queue()
    count = 5
    for input_data in sql_inv_list:
        make_model = input_data[2] + " " + input_data[3]
        PartName = input_data[4]
        part = input_data[7]
        part_type = input_data[6]
        interchange = input_data[8]
        inputs = get_inputs(make_model, PartName, parts, models)
        car_input = inputs[0]
        part_input = inputs[1]
        start_year = str(input_data[5]).split('.')[0]
        id = input_data[0]
        des = input_data[1]
        list_1.append((car_input, part_input, start_year, id, des))
    for i in list_1:
        que.put(i)
    for i in range(count):
        th_driver = web_driver()
        t = Thread(target=search, args=(th_driver, que))
        t.start()
    que.join()
