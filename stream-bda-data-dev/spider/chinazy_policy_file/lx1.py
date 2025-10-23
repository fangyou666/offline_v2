import logging
from datetime import datetime
from DrissionPage import ChromiumPage, ChromiumOptions

log_file = f'spider_amap_weather_{datetime.now():%Y%m%d_%H%M%S}.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file, encoding='utf-8'),
                              logging.StreamHandler()])

# 1. 手动先启 Chrome（管理员 CMD）
#    "C:\Program Files\Google\Chrome\Application\chrome.exe"
#     --remote-debugging-port=9222 --user-data-dir=D:\chrome_debug

co = ChromiumOptions()
co.set_local_port(9222)
dp = ChromiumPage(addr_or_opts=co)

try:
    dp.get('https://www.chinazy.org/zcfg.htm')
    dp.wait(2)
    print(dp.html)
except Exception as e:
    logging.error('抓取失败', exc_info=True)