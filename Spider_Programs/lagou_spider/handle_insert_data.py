import time
from collections import Counter
from sqlalchemy import func

from Spider_Programs.lagou_spider.create_lagou_tables import Lagoutables
from Spider_Programs.lagou_spider.create_lagou_tables import Session


class HandleLagouData(object):
    def __init__(self):
        # 实例化Session信息
        self.mysql_session = Session()
        self.date = time.strftime("%Y-%m-%d", time.localtime())

    # 定义数据的存储方法
    def insert_item(self, item):
        # 今天
        date = time.strftime("%Y-%m-%d", time.localtime())
        # 存储的数据结构
        data = Lagoutables(
            # 岗位ID
            positionID=item['positionId'],
            # 经度
            longitude=item['longitude'],
            # 维度
            latitude=item['latitude'],
            # 岗位名称
            positionName=item['positionName'],
            # 工作年限
            workYear=item['workYear'],
            # 学历
            education=item['education'],
            # 岗位性质
            jobNature=item['jobNature'],
            # 公司类型
            financeStage=item['financeStage'],
            # 公司规模
            companySize=item['companySize'],
            # 业务方向
            industryField=item['industryField'],
            # 所在城市
            city=item['city'],
            # 岗位标签
            positionAdvantage=item['positionAdvantage'],
            # 公司简称
            companyShortName=item['companyShortName'],
            # 公司全称
            companyFullName=item['companyFullName'],
            # 公司所在区
            district=item['district'],
            # 公司福利标签
            companyLabelList=','.join(item['companyLabelList']),
            # 工资
            salary=item['salary'],
            # 抓取日期
            crawl_date=date
        )
        # filter过滤 通过每一天不能有重复的岗位id来去重
        # 再存储数据之前，先来查询一下表里是否有这条岗位信息
        query_result = self.mysql_session.query(Lagoutables).filter(Lagoutables.crawl_date == date,
                                                                    Lagoutables.positionID == item[
                                                                        'positionId']).first()
        if query_result:
            print("该岗位信息已存在%s:%s:%s" % (item['positionId'], item['city'], item['positionName']))
        else:
            # 插入数据
            self.mysql_session.add(data)
            # 提交数据到数据库
            self.mysql_session.commit()
            print("新增岗位信息%s" % item['positionId'])

    def query_industryfield_result(self):
        info = {}
        # all()方法返回数据 查询今日抓取到的行业信息数据
        result = self.mysql_session.query(Lagoutables.industryField).filter(Lagoutables.crawl_date == self.date).all()
        # result = self.mysql_session.query(Lagoutables.industryField).all()
        # 通过列表生成式来遍历 x[0]取得每个元祖 通过,来进行分割 (',')[0]取得每个元祖的第一个数据
        result_list1 = [x[0].split(',')[0] for x in result]
        # 通过Counter来记录每个标签出现的次数 并按从大到小排序
        # print(Counter(result_list1))
        # items()将字典变为个列表 限制只有在x[1]大于60时才使用，即职位数量大于60时才显示
        result_list2 = [x for x in Counter(result_list1).items() if x[1] > 60]
        # 填充的是series里面的data  data为一个列表里边包含着很多字典
        data = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in data]
        info['x_name'] = name_list
        info['data'] = data
        return info

    # 查询薪资情况
    def query_salary_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.salary).filter(Lagoutables.crawl_date == self.date).all()
        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items() if x[1] > 45]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 查询工作年限情况
    def query_workyear_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.workYear).filter(Lagoutables.crawl_date == self.date).all()
        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2 if x[1] > 15]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 查询学历信息
    def query_education_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.education).filter(Lagoutables.crawl_date == self.date).all()
        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 岗位发布数量,折线图
    def query_job_result(self):
        info = {}
        result = self.mysql_session.query(Lagoutables.crawl_date, func.count('*').label('c')).group_by(
            Lagoutables.crawl_date).all()
        result1 = [{"name": x[0], "value": x[1]} for x in result]
        name_list = [name['name'] for name in result1]
        info['x_name'] = name_list
        info['data'] = result1
        return info

    # 根据城市计数
    def query_city_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.city, func.count('*').label('c')).filter(
            Lagoutables.crawl_date == self.date).group_by(Lagoutables.city).all()
        result1 = [{"name": x[0], "value": x[1]} for x in result]
        name_list = [name['name'] for name in result1]
        info['x_name'] = name_list
        info['data'] = result1
        return info

    # 融资情况
    def query_financestage_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.financeStage).filter(Lagoutables.crawl_date == self.date).all()
        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 公司规模
    def query_companysize_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.companySize).filter(Lagoutables.crawl_date == self.date).all()
        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 任职情况
    def query_jobNature_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.jobNature).filter(Lagoutables.crawl_date == self.date).all()
        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 抓取数量
    def count_result(self):
        info = {}
        info['all_count'] = self.mysql_session.query(Lagoutables).count()
        info['today_count'] = self.mysql_session.query(Lagoutables).filter(Lagoutables.crawl_date == self.date).count()
        return info


# 创建类的实例
lagou_mysql = HandleLagouData()
