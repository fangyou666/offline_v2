import logging
import time
import pymysql
from datetime import datetime
from lxml import html
from utils.ChromeDebugControllerUtils import ChromeDebugController

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spider_jd_multi_page.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# MySQL 数据库配置
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",   # 替换为实际密码
    "db": "jd_spider",
    "charset": "utf8mb4"
}

def get_db_connection():
    try:
        conn = pymysql.connect(** MYSQL_CONFIG)
        logger.info("成功连接到MySQL数据库")
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败：{str(e)}")
        return None

def insert_crawl_log(conn, total, success, status, error_msg=""):
    if not conn:
        return None
    try:
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO jd_crawl_log (crawl_time, total_products, success_products, status, error_msg)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (datetime.now(), total, success, status, error_msg))
            conn.commit()
            return cursor.lastrowid
    except Exception as e:
        conn.rollback()
        logger.error(f"日志插入失败：{str(e)}")
        return None

def insert_or_update_dimension(conn, products):
    if not conn or not products:
        return {}
    link_to_id = {}
    try:
        with conn.cursor() as cursor:
            for p in products:
                link = p["link"]
                name = p["name"]
                cursor.execute("SELECT product_id FROM jd_product_dim WHERE product_link = %s", (link,))
                result = cursor.fetchone()
                if result:
                    product_id = result[0]
                    cursor.execute("""
                        UPDATE jd_product_dim 
                        SET product_name = %s, last_crawl_time = %s 
                        WHERE product_id = %s
                    """, (name, datetime.now(), product_id))
                else:
                    cursor.execute("""
                        INSERT INTO jd_product_dim (product_name, product_link, first_crawl_time)
                        VALUES (%s, %s, %s)
                    """, (name, link, datetime.now()))
                    product_id = cursor.lastrowid
                link_to_id[link] = product_id
            conn.commit()
            logger.info(f"维度表处理完成，共 {len(link_to_id)} 条记录")
            return link_to_id
    except Exception as e:
        conn.rollback()
        logger.error(f"维度表处理失败：{str(e)}")
        return {}

def insert_fact_table(conn, products, link_to_id, log_id):
    if not conn or not products or not link_to_id or not log_id:
        return 0
    try:
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO jd_product_fact (product_id, log_id, price, crawl_time)
            VALUES (%s, %s, %s, %s)
            """
            data = []
            for p in products:
                product_id = link_to_id.get(p["link"])
                if not product_id:
                    continue
                price_str = p["price"].replace("¥", "").replace(",", "").strip()
                price = float(price_str) if price_str else None
                data.append((product_id, log_id, price, datetime.now()))
            cursor.executemany(sql, data)
            conn.commit()
            logger.info(f"事实表插入 {len(data)} 条价格记录")
            return len(data)
    except Exception as e:
        conn.rollback()
        logger.error(f"事实表插入失败：{str(e)}")
        return 0

def crawl_jd_page(drission_page, page_num):
    """爬取单页商品数据（适配实际页面元素）"""
    logger.info(f"开始爬取第 {page_num} 页")
    # 滚动加载商品
    drission_page.run_js("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)  # 等待加载

    tree = html.fromstring(drission_page.html)
    # 商品容器XPath（根据实际页面调整）
    product_elems = tree.xpath('//li[contains(@class, "gl-item")]')
    jd_products = []
    for idx, elem in enumerate(product_elems, 1):
        # 商品名称（匹配<div class="jDesc">下的a标签）
        name_elems = elem.xpath('.//div[contains(@class, "jDesc")]/a/text()')
        name = name_elems[0].strip() if name_elems else f"未知商品_{page_num}_{idx}"

        # 商品价格（匹配实际价格标签）
        price_elems = elem.xpath('.//div[contains(@class, "p-price")]/span[contains(@class, "price")]/text()')
        price = price_elems[0].strip() if price_elems else "0.00"

        # 商品链接（匹配名称标签的href）
        link_elems = elem.xpath('.//div[contains(@class, "jDesc")]/a/@href')
        link = f"https:{link_elems[0]}" if link_elems and link_elems[0].startswith("//") else (link_elems[0] if link_elems else "")

        jd_products.append({
            "name": name,
            "price": price,
            "link": link
        })
    logger.info(f"第 {page_num} 页提取到 {len(jd_products)} 个商品")
    return jd_products

def click_next_page(drission_page):
    """点击下一页按钮（匹配含“下一页”文本的按钮）"""
    try:
        # 定位包含“下一页”文本的按钮并点击
        js = """
            const nextBtn = document.evaluate('//a[contains(text(), "下一页")]', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
            if (nextBtn) {
                nextBtn.click();
                return true;
            }
            return false;
        """
        return drission_page.run_js(js)
    except Exception as e:
        logger.error(f"点击下一页失败：{str(e)}")
        return False

def crawl_jd_multi_page():
    chrome_debug = ChromeDebugController(
        user_data_dir=r'C:\专高六\IDEAFIle\offline_v2\stream-bda-data-dev\edge_data'
    )

    drission_page = None
    db_conn = None
    log_id = None
    total_products = 0
    success_products = 0
    error_msg = ""
    all_products = []
    total_pages = 98  # 总页数
    current_page = 1

    try:
        # 1. 连接数据库
        db_conn = get_db_connection()
        if not db_conn:
            error_msg = "数据库连接失败"
            return

        # 2. 启动浏览器并访问京东首页
        if chrome_debug.start():
            drission_page = chrome_debug.connect_drissionpage()
        else:
            error_msg = "浏览器启动失败"
            return

        if drission_page:
            jd_url = "https://mall.jd.com/view_search-2286219.html"
            drission_page.get(jd_url)
            logger.info(f"已访问京东页面：{jd_url}")

            # 预留登录时间（若需要）
            print("请在30秒内手动登录（若需要）...")
            time.sleep(30)

            # 3. 多页爬取循环
            while current_page <= total_pages:
                page_products = crawl_jd_page(drission_page, current_page)
                all_products.extend(page_products)
                total_products += len(page_products)

                # 点击“下一页”按钮
                if current_page < total_pages:
                    if click_next_page(drission_page):
                        time.sleep(5)  # 延长等待时间，确保页面加载完成
                        current_page += 1
                    else:
                        logger.warning(f"第 {current_page} 页后未找到下一页按钮，停止爬取")
                        break
                else:
                    break

        # 4. 数据入库
        if all_products:
            # 4.1 插入日志表
            log_id = insert_crawl_log(db_conn, total_products, 0, "processing")
            if not log_id:
                error_msg = "日志表插入失败"
                raise Exception(error_msg)

            # 4.2 处理维度表
            link_to_id = insert_or_update_dimension(db_conn, all_products)
            if not link_to_id:
                error_msg = "维度表处理失败"
                raise Exception(error_msg)

            # 4.3 插入事实表
            success_products = insert_fact_table(db_conn, all_products, link_to_id, log_id)

            # 4.4 更新日志表状态为成功
            insert_crawl_log(db_conn, total_products, success_products, "success")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"爬取失败：{error_msg}", exc_info=True)
        if db_conn and log_id:
            insert_crawl_log(db_conn, total_products, success_products, "fail", error_msg)

    finally:
        print("爬取结束，10秒后关闭浏览器...")
        time.sleep(10)
        if chrome_debug:
            chrome_debug.close()
        if db_conn:
            db_conn.close()
            logger.info("数据库连接已关闭")
        logger.info(f"京东多页爬虫结束 | 总商品数：{total_products} | 成功保存：{success_products}")

if __name__ == "__main__":
    crawl_jd_multi_page()


