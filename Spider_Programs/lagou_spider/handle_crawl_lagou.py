import requests
import re
import time
import json
import multiprocessing
from Spider_Programs.lagou_spider.handle_insert_data import lagou_mysql


class HandleLaGou(object):
    def __init__(self):
        # 使用session保存cookie信息
        self.lagou_session = requests.session()
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                          ' Chrome/73.0.3683.75 Safari/537.36'
        }
        self.city_list = ""

    # 获取全国所有城市列表的方法
    def handle_city(self):
        city_search = re.compile(r'www\.lagou\.com\/.*\/">(.*?)</a>')
        city_url = "https://www.lagou.com/jobs/allCity.html"
        city_result = self.handle_request(method="GET", url=city_url)
        # 使用正则获取城市列表
        self.city_list = city_search.findall(city_result)
        # print(self.city_list)
        # 清除session里的cookie信息
        self.lagou_session.cookies.clear()

    def handle_city_job(self, city):
        first_request_url = "https://www.lagou.com/jobs/list_Python?px=default&city=%s" % city
        first_response = self.handle_request(method="GET",
                                             url=first_request_url)  # 并不用headers，因为handle_request方法里已经有传headers
        total_page_search = re.compile(r'class="span\stotalNum">(\d+)</span>')
        try:
            total_page = total_page_search.search(first_response).group(1)
            # 页数
            # print(total_page)
        # 由于没有岗位信息造成的Exception
        except:
            return
        else:
            for i in range(1, int(total_page) + 1):
                data = {
                    'pn': i,
                    'kd': "python",
                }
                page_url = "https://www.lagou.com/jobs/positionAjax.json?px=default&city=%s" \
                           "&needAddtionalResult=false" % city
                referer_url = "https://www.lagou.com/jobs/list_Python?px=default&city=%s" % city
                # referer的URL还需要进行encode
                self.header['Referer'] = referer_url.encode()
                response = self.handle_request(method="POST", url=page_url, data=data, info=city)
                lagou_data = json.loads(response)
                job_list = lagou_data['content']['positionResult']['result']
                for job in job_list:
                    lagou_mysql.insert_item(job)
                print(response)
        # print(first_response)

    # 定义请求方法
    def handle_request(self, method, url, data=None, info=None):
        while True:
            # 加入阿布云的动态代理 隐藏本机ip地址 隐藏爬虫  由于没有自己的代理 故不使用代理 若使用了代理 在get/post中加入proxies=proxy 并且加上try except
            # proxyinfo = "http://%s:%s@%s:%s" % ('H1V32R6470A7G90D', 'CD217C660A9143C3', 'http-dyn.abuyun.com', '9020')
            # proxy = {
            #     "http": proxyinfo,
            #     "https": proxyinfo
            # }

            if method == 'GET':
                response = self.lagou_session.get(url=url, headers=self.header, timeout=6)
            elif method == 'POST':
                response = self.lagou_session.post(url=url, headers=self.header, data=data, timeout=6)

            # response.encoding = 'utf8'
            if '频繁' in response.text:
                print(response.text)
                # 先清除cookie信息
                self.lagou_session.cookies.clear()
                # 重新获取cookie信息
                first_request_url = "https://www.lagou.com/jobs/list_Python?px=default&city=%s" % info
                self.handle_request(method="GET", url=first_request_url)  # 并不用headers，因为handle_request方法里已经有传headers
                # 模拟人每10s浏览一页
                time.sleep(10)
                continue
            return response.text


if __name__ == '__main__':
    lagou = HandleLaGou()
    # 获取所有城市
    lagou.handle_city()
    pool = multiprocessing.Pool(2)  # 进程池
    # 使用非阻塞进程池  引用进程池的方法加速抓取
    for city in lagou.city_list:
        pool.apply_async(lagou.handle_city_job, args=(city,))  # 使用args将参数传递进去
    pool.close()  # 关闭进程池
    pool.join()

    # 没用进程的方法
    # for city in lagou.city_list:
    #     print(city)
    #     lagou.handle_city_job(city)
    #     break
