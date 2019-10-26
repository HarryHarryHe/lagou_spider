import pymysql
import xlwt as xlwt

conn = pymysql.connect(host='127.0.0.1', user='root', passwd='19990428', db="lagou", charset="utf8")
curs = conn.cursor()

sql = '''select * from lagou_data'''
curs.execute(sql)
rows = curs.fetchall()

w = xlwt.Workbook(encoding='utf-8')
style = xlwt.XFStyle()  # 初始化样式
font = xlwt.Font()  # 为样式创建字体
font.name = "微软雅黑"  # 如果是 python2 ，需要这样写 u"微软雅黑"
style.font = font  # 为样式设置字体
ws = w.add_sheet("python岗位信息", cell_overwrite_ok=True)

# 将 title 作为 Excel 的列名
title = "id,岗位ID,经度,纬度,岗位名称,工作年限,学历,岗位性质,公司类型,公司规模,业务方向,所在城市,岗位标签,公司简称,公司全称,公司所在区,公司福利标签,工资,抓取日期"
title = title.split(",")
for i in range(len(title)):
    ws.write(0, i, title[i], style)
# 开始写入数据库查询到的数据
for i in range(len(rows)):
    row = rows[i]
    for j in range(len(row)):
        if row[j]:
            item = row[j]
            ws.write(i + 1, j, item, style)

# 写文件完成，开始保存xls文件
path = './python_jobs.xls'
w.save(path)
conn.close()
