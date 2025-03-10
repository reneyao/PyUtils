# 绩效模块
from typing import Literal
from PyUtils import Database, Calendar


def performance_product_period(type: Literal["Days", "Year"], period=7):
    """
    产品N日均净值

    - 前N个交易日日均规模
    - 今年以来日均规模
    """

    if type == "Year":
        sql = """
            SELECT fundname, AVG(assets) AS dailyAssets
            FROM performance_only
            WHERE YEAR(datadate) = YEAR(CURDATE())
            GROUP BY fundname;
            """
    else:
        if period is None or not isinstance(period, int) or period <= 0:
            raise ValueError("参数period必须为正整数")
        days = period
        res = Calendar.get_period_date(days)
        start_date = res.iloc[0]["calendarDate"]
        sql = f"""
            SELECT fundname, AVG(assets) AS dailyAssets
            FROM performance_only
            WHERE datadate >= '{start_date}'
            GROUP BY fundname;
        """

    res = Database.query_pd("mysql18", sql)
    return res
