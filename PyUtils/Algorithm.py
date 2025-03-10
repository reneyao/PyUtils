from datetime import datetime
import numpy as np
import pandas as pd
from PyUtils import Database
from Calendar import get_last_day_of_year


def IfNULL(indicator1, indicator2):
    """
    判断指标一是否为空值

    参数:
    indicator1: 需要被判断的指标
    indicator2: 常数或空值

    返回:
    min: 所有指标中的最大值
    """
    if indicator1 is None or indicator1 == "":
        return indicator2
    else:
        return indicator1


def Power(indicator, exponent):
    """
    计算指标的乘幂

    参数:
    indicator: 需要计算乘幂的指标
    exponent: 乘幂指数

    返回:
    result: 指标的乘幂结果
    """
    # 计算指标的乘幂
    result = indicator**exponent
    return result


def abs_value(indicator):
    """
    返回指标的绝对值

    参数:
    indicator: 需要计算绝对值的指标

    返回:
    abs_indicator: 指标的绝对值
    """
    # 返回指标的绝对值
    abs_indicator = abs(indicator)
    return abs_indicator


def Not(condition):
    """
    根据条件返回0或1,或者保留空值

    参数:
    condition: 条件表达式或数值指标

    返回:
    result: 如果条件为真值(非0数值),返回0;否则返回1;如果条件为空,返回空
    """
    # 如果条件是真值（非0数值），返回0
    if condition:
        result = 0
    # 如果条件是假值（0或False），返回1
    else:
        result = 1
    return result


def And(*conditions):
    """
    判断所有输入条件是否都为真(非0)

    参数:
    conditions: 至少两个，最多八个条件表达式或数值指标

    返回:
    result: 如果所有条件都为真(非0),返回1;否则返回0
    """
    # 检查输入条件数量
    if len(conditions) < 2 or len(conditions) > 8:
        raise ValueError("需要至少2个,最多8个输入条件")

    # 检查所有条件是否都为真
    for condition in conditions:
        if not condition:
            return 0
    return 1


def Or(*conditions):
    """
    判断所有输入条件是否都为真(非0)

    参数:
    conditions: 至少两个，最多八个条件表达式或数值指标

    返回:
    result: 如果至少一个条件为真(非0),返回1;否则返回0
    """
    # 检查输入条件的数量是否符合要求
    if len(conditions) < 2 or len(conditions) > 8:
        raise ValueError("需要提供2到8个条件")

    # 遍历所有条件，检查是否有任意一个条件为真(非0)
    for condition in conditions:
        if condition:  # 如果条件为真(非0)
            return 1

    # 如果所有条件都为假(0)，则返回0
    return 0


def Mod(value, divisor):
    """
    计算除法的余数

    参数:
    value: 被除数
    divisor: 除数，可以是指标或常数

    返回:
    result: 除法的余数
    """
    try:
        # 计算余数
        result = value % divisor
        return result
    except TypeError:
        raise TypeError("输入参数必须是可以进行除法运算的数值类型")
    except ZeroDivisionError:
        raise ZeroDivisionError("除数不能为0")


def Anual(ticker: str, indicator: str, shift_years: int):
    """
    获取指定股票代码和指标的年报数据

    参数:
    ticker: 股票代码
    indicator: 需要查询的指标名称
    shift_years: 前移年数

    返回:
    数值: 查询到的指标值,如果查询不到则返回None
    """
    today = datetime.now().date()
    target_year = today.year - shift_years - 1
    last_day_of_target_year = get_last_day_of_year(target_year)

    # 构建SQL查询语句
    sql = f"""
    SELECT {ticker}, {indicator} 
    FROM fdmt_is_2018 
    WHERE endDate = '{last_day_of_target_year.strftime('%Y-%m-%d')}'
    AND reportType = 'A'
    AND ticker = '{ticker}'
    ORDER BY publishDate DESC
    LIMIT 1
    """
    # 使用query_pd函数查询数据
    df = Database.query_pd("server93Api", sql)

    # 从DataFrame中获取指标值
    if not df.empty:
        return df.iloc[0][indicator]
    else:
        return None


def PercentRank(ticker, indicator, N):
    """
    计算给定指标在过去N个交易日中的排名百分位

    参数:
    ticker: 股票代码
    indicator: 指标名称
    N: 过去N天的天数

    返回:
    百分位排名: 介于0和1之间的小数
    """
    # 编写SQL查询语句
    sql = f"""
        SELECT {indicator}, tradeDate
        FROM mkt_equd
        WHERE ticker = '{ticker}'
        ORDER BY tradeDate DESC
        LIMIT {N+1}
    """

    # 使用query_pd函数查询数据
    df = Database.query_pd("server93Api", sql)

    # 检查DataFrame是否为空
    if df.empty:
        raise ValueError("No data found for the given period and ticker.")
    # 获取当天的指标数值
    today_value = df.iloc[0][indicator]

    df["rank"] = df[indicator].rank(ascending=True, method="min") - 1
    today_rank = df.loc[df.iloc[0].name, "rank"]
    percent_rank = today_rank / N
    print(df)
    return percent_rank


def refq(indicator, n, fill_option, table, ticker, server_name="server93Api"):
    """
    获取指定股票的财报指标，并根据参数处理空值。

    参数:
    revenue_col: 财报指标的列名。
    n: 往前N个季度。
    fill_option: 补全选项,0表示往前找4个季度补全空值,1表示保留空值,2表示空值转换成0。
    server_name: 数据库服务器名称,默认为 'server93Api'。
    table: 查询表格名称,具体可以为fdmt_xx_2018,bs,cf,is可选。
    ticker: 股票代码。

    返回:
    指定季度的财报指标值。
    """
    # 获取当前日期
    current_date = datetime.now().strftime("%Y-%m-%d")

    # 构建SQL查询
    query = f"""
    WITH RankedReports AS (
        SELECT 
            endDate, 
            {indicator}, 
            publishDate,
            ROW_NUMBER() OVER (PARTITION BY endDate ORDER BY ID DESC) AS rn
        FROM {table}
        WHERE 
            ticker = '{ticker}' AND
            endDate <= '{current_date}' AND
            (endDate LIKE '%-03-31' OR endDate LIKE '%-06-30' OR endDate LIKE '%-09-30' OR endDate LIKE '%-12-31')
    )
    SELECT *
    FROM RankedReports
    WHERE rn = 1
    ORDER BY endDate DESC
    LIMIT {n+1}
    """
    # 使用query_pd函数查询数据
    df = Database.query_pd(server_name, query)
    print(df)
    # 确保endDate和publishDate是datetime类型
    df["endDate"] = pd.to_datetime(df["endDate"])
    df["publishDate"] = pd.to_datetime(df["publishDate"])

    # 初始化结果变量
    result = None

    # 如果DataFrame不为空，选择最后一行的revenue_col值
    if not df.empty:
        result = df.iloc[-1][indicator]

    # 处理空值
    if result is None:
        if fill_option == 0:
            # 往前找4个季度补全空值
            for i in range(min(len(df), 4)):
                if pd.notna(df.iloc[-i - 1][indicator]):
                    result = df.iloc[-i - 1][indicator]
                    break
        elif fill_option == 1:
            result = None
        elif fill_option == 2:
            result = 0
    return result


def Stdev(indicator, n, table, ticker, server_name="server93Api"):
    """
    获取指定股票的财报指标,返回股票最近N个季度指标之标准方差。

    参数:
    revenue_col: 财报指标的列名。
    n: 最近N个季度。
    table: 查询表格名称,具体可以为fdmt_xx_2018,bs,cf,is可选。
    ticker: 股票代码。
    server_name: 数据库服务器名称,默认为 'server93Api'。

    返回:
    指定季度的财报指标标准差
    """
    # 获取当前日期
    current_date = datetime.now().strftime("%Y-%m-%d")

    # 构建SQL查询
    query = f"""
    WITH RankedReports AS (
        SELECT 
            endDate, 
            {indicator}, 
            publishDate,
            ROW_NUMBER() OVER (PARTITION BY endDate ORDER BY ID DESC) AS rn
        FROM {table}
        WHERE 
            ticker = '{ticker}' AND
            endDate <= '{current_date}' AND
            (endDate LIKE '%-03-31' OR endDate LIKE '%-06-30' OR endDate LIKE '%-09-30' OR endDate LIKE '%-12-31')
    )
    SELECT *
    FROM RankedReports
    WHERE rn = 1
    ORDER BY endDate DESC
    LIMIT {n}
    """
    # 使用query_pd函数查询数据
    df = Database.query_pd(server_name, query)
    print(df)

    df[indicator] = df[indicator].astype(float)
    stdev = df[indicator].std()

    return stdev


def AccuQ(indicator, n, table, ticker, server_name="server93Api"):
    """
    获取指定股票的对应年份财报指标,并将其加和

    参数:
    indicator: 财报指标的列名。
    n: 前移n个年份。
    table: 查询表格名称,具体可以为fdmt_xx_2018,bs,cf,is可选。
    ticker: 股票代码。
    server_name: 数据库服务器名称,默认为 'server93Api'。

    返回:
    指定年度指标和
    """
    # 获取当前日期
    current_date = datetime.now()
    if n == 0:
        start = datetime(current_date.year - n, 1, 1).strftime("%Y-%m-%d")
        end = current_date.strftime("%Y-%m-%d")
    else:
        start = datetime(current_date.year - n, 1, 1).strftime("%Y-%m-%d")
        end = datetime(current_date.year - n, 12, 31).strftime("%Y-%m-%d")
    # 构建SQL查询
    query = f"""
    WITH RankedReports AS (
        SELECT 
            endDate, 
            {indicator}, 
            publishDate,
            ROW_NUMBER() OVER (PARTITION BY endDate ORDER BY ID DESC) AS rn
        FROM {table}
        WHERE 
            ticker = '{ticker}' AND
            endDate <= '{end}' AND
            endDate >= '{start}' AND
            (endDate LIKE '%-03-31' OR endDate LIKE '%-06-30' OR endDate LIKE '%-09-30' OR endDate LIKE '%-12-31')
    )
    SELECT *
    FROM RankedReports
    WHERE rn = 1
    ORDER BY endDate DESC
    """
    # 使用query_pd函数查询数据
    df = Database.query_pd(server_name, query)
    result = df.iloc[0][indicator]
    return result


# 需要额外强调的是,虽然需求中所想的是把一年累积的所有值加和,但基于数据层面,其结果已经做了累加,因此只需要调取最新的一期的数据即可


def LastValue(
    indicator: str, conditions: list, table: str, ticker: int, server_name="server93Api"
):
    """
    获取指定股票的符合条件的最新数据

    参数:
    indicator: 财报指标的列名。
    condition: 用空格间隔的条件,列表可存储多个条件
    table: 查询表格名称,具体可以为mkt_equd。
    ticker: 股票代码。
    server_name: 数据库服务器名称,默认为 'server93Api'。

    返回:
    指定条件的指标值
    """
    condition_clauses = []
    for condition in conditions:
        col, op, val = condition.split()
        val = float(val)  # 将值转换为浮点数
        condition_clauses.append(f"{col} {op} {val}")

    query = f"""
    SELECT {indicator}
    FROM {table}
    WHERE ticker = {ticker}
      AND {' AND '.join(condition_clauses)}
    ORDER BY tradeDate DESC
    LIMIT 1
    """
    # 使用query_pd函数查询数据
    df = Database.query_pd(server_name, query)
    # 获取指标值
    if not df.empty:
        last_value = df.iloc[0][indicator]
        return last_value
    else:
        return None


def MA(indicator, n, table, ticker, server_name="server93Api"):
    """
    获取指定股票的指标均值

    参数:
    indicator: 财报指标的列名。
    condition: 用空格间隔的条件,列表可存储多个条件
    table: 查询表格名称,具体可以为fdmt_xx_2018,bs,cf,is可选。
    ticker: 股票代码。
    server_name: 数据库服务器名称,默认为 'server93Api'。

    返回:
    指定条件的指标值
    """
    current_day = datetime.now().strftime("%Y-%m-%d")
    if n == 0:
        query = f"""
        SELECT {indicator}
        FROM {table}
        WHERE ticker = '{ticker}'
        AND tradingDate <= '{current_day}'
        ORDER BY tradingDate DESC
        """
        df = Database.query_pd(server_name, query)

        mva = np.mean(df[indicator])

        return mva
    else:
        query = f"""
        SELECT {indicator},tradeDate
        FROM {table}
        WHERE ticker = '{ticker}'
        AND tradeDate < '{current_day}'
        ORDER BY tradeDate DESC
        LIMIT {n}
        """
        df = Database.query_pd(server_name, query)
        mva = np.mean(df[indicator])
        print(df)
        return mva
