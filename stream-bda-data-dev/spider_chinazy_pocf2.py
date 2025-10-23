import logging
from datetime import datetime

from utils.ChromeDebugControllerUtils import ChromeDebugController

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spider_amap_weather.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

chrome_debug = ChromeDebugController(
    user_data_dir=r'C:\专高六\IDEAFIle\offline_v2\stream-bda-data-dev\edge_data'
)

drission_page = None
if chrome_debug.start():
    drission_page = chrome_debug.connect_drissionpage()

if drission_page:
    drission_page.get("https://www.chinazy.org/zcfg.htm")
    drission_page.wait(2)  # 等待页面加载

    # 打印完整页面内容
    print("="*50)
    print("页面标题：", drission_page.title)
    print("="*50)
    print("完整页面HTML内容：\n", drission_page.html)
    print("="*50)

else:
    logger.error("❌ 浏览器启动或连接失败")

chrome_debug.close()