from crawler import crawlData
from cleaner import cleanData
from loader import loadDataToDatabase
import schedule
import time

def etl():
    print("Running ETL...")
    crawlData()
    cleanData()
    loadDataToDatabase()

def run_etl(): 
    schedule.every(30).days.do(etl)
    while True: 
        schedule.run_pending() 
        time.sleep(1)

def main():
    etl()
    run_etl()

main()