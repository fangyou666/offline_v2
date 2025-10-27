import logging
from lxml import html
from datetime import datetime

from utils.ChromeDebugControllerUtils import ChromeDebugController

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spider_amap_weather.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 初始化Edge调试控制器
chrome_debug = ChromeDebugController(
    user_data_dir=r'C:\专高六\IDEAFIle\offline_v2\stream-bda-data-dev\edge_data'
)

drission_page = None
if chrome_debug.start():
    drission_page = chrome_debug.connect_drissionpage()

if drission_page:
    # 访问目标网页
    drission_page.get("https://www.chinazy.org/zcfg.htm")
    drission_page.wait(2)  # 等待页面加载

    # ========== 打印完整HTML ==========
    print("="*50)
    print("完整页面HTML内容：\n", drission_page.html)
    print("="*50)

    # ========== 提取结构化数据并打印 ==========
    tree = html.fromstring(drission_page.html)
    policy_elements = tree.xpath('//li/a[contains(@href, "info/") and @title]')

    policies = []
    for elem in policy_elements:
        link = elem.xpath('./@href')[0] if elem.xpath('./@href') else ''
        name = elem.xpath('./@title')[0] if elem.xpath('./@title') else ''
        if not name:
            full_text = ''.join(elem.xpath('./text()')).strip()
            if len(full_text) > 10:
                name = full_text[10:].strip()
        if link and name:
            policies.append({
                'link': link,
                'name': name
            })

    print("提取的政策数据：")
    for policy in policies:
        print(policy)

else:
    logger.error("❌ 浏览器启动或连接失败")

# 关闭浏览器
chrome_debug.close()

