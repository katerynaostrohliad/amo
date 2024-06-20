import pandas as pd
import schedule
import time
from amo import main_amo

schedule.every().day.at('08:00').do(main_amo)


while True:
    schedule.run_pending()
    time.sleep(1)
