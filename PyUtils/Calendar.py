from datetime import datetime, timedelta, time
import pandas as pd
from PyUtils import Database
from typing import Optional, Literal


def is_valid_date(date_str) -> bool:
    """判断输入的字符串格式是否为%Y-%m-%d"""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        raise False


def get_last_friday(date_str) -> str:
    """
    返回上周五的日期
    """
    if not is_valid_date(date_str):
        raise ValueError(f"日期格式错误")

    input_date = datetime.strptime(date_str, "%Y-%m-%d")
    weekday = input_date.weekday()
    days_to_last_friday = (weekday + 2) % 7 + 1
    last_friday = input_date - timedelta(days=days_to_last_friday)
    return last_friday.strftime("%Y-%m-%d")


def is_market_running(
    scale: Optional[Literal["inner"]] = None,
    open_tm: Optional[time] = None,
    close_tm: Optional[time] = None,
) -> bool:
    """
    判断当前市场是否开市

    参数:
    scale: 可选参数，如果为"inner"则不检查是否为交易日
    open_tm: 可选参数，开市时间，默认为 time(9, 30)
    close_tm: 可选参数，收市时间，默认为 time(15, 0)

    返回:
    bool: 如果当前时间在交易时间内（且为交易日，当scale不为"inner"时）返回True，否则返回False
    """
    now = datetime.now()
    now_tm = now.time()
    # 设置默认交易时间
    open_tm = open_tm if open_tm is not None else time(9, 30)
    close_tm = close_tm if close_tm is not None else time(15, 0)
    lunch_start = time(11, 30)
    lunch_end = time(13, 0)

    if scale is None:
        now_date = now.strftime("%Y-%m-%d")
        is_trading_day = Database.query(
            "server93Api",
            "SELECT isOpen FROM trade_cal WHERE exchangeCD = 'XSHG' AND calendarDate = '"
            + now_date
            + "'",
        ).fetchone()[0]

        if is_trading_day and (
            open_tm <= now_tm < close_tm and not (lunch_start < now_tm < lunch_end)
        ):
            return True

    elif open_tm <= now_tm < close_tm and not (lunch_start < now_tm < lunch_end):
        return True

    return False


def prev_trading_date(date: str):
    """
    返回上一个交易日日期

    参数:
    date: 日期字符串，格式为 YYYY-MM-DD

    返回:
    日期字符串，格式为 YYYY-MM-DD
    """
    if not is_valid_date(date):
        raise ValueError(f"日期格式错误")

    res = Database.query(
        "server93Api",
        "SELECT prevTradeDate FROM trade_cal WHERE exchangeCD = 'XSHG' AND calendarDate = '"
        + date
        + "'",
    ).fetchone()[0]
    return res


def is_trading_day(date: str) -> bool:
    """
    判断是否为交易日

    参数：
    date: 日期字符串，格式为 YYYY-MM-DD

    返回：
    bool: 交易日返回 True, 否则返回 False
    """

    if not is_valid_date(date):
        raise ValueError(f"日期格式错误")

    res = Database.query(
        "server93Api",
        "SELECT isOpen FROM trade_cal WHERE exchangeCD = 'XSHG' AND calendarDate = '"
        + date
        + "'",
    ).fetchone()[0]

    return 1 == res


def get_sql_df(date: str):
    """
    获取sql表格并存储为df

    参数:
    date: 日期字符串，格式为 YYYY-MM-DD

    返回:
    dataframe: 存储记录日期信息
    """
    if not is_valid_date(date):
        raise ValueError(f"日期格式错误")

    date_obj = datetime.strptime(date, "%Y-%m-%d")

    # 计算前一年的日期
    previous_year_date_obj = date_obj - timedelta(days=365)

    # 将计算得到的日期转换回字符串格式
    previous_year_date_str = previous_year_date_obj.strftime("%Y-%m-%d")

    df = Database.query_pd(
        "server93Api",
        f"SELECT * FROM trade_cal WHERE exchangeCD = 'XSHG' AND calendarDate BETWEEN '{previous_year_date_str}' and '{date}' ",
    )
    return df


def get_previous_trading_day(df, date_str: str) -> datetime.date:
    """获取给定日期的前一个交易日"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    prev_trade_date = df[df["CALENDAR_DATE"] == date_obj]["PREV_TRADE_DATE"].iloc[0]
    return prev_trade_date


def get_last_trading_day_of_period(df, date_str: str, period: str) -> datetime.date:
    """获取给定日期所在周期的最后一个交易日"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    last_trading_day = None

    if period == "week":
        current_week_end = df[df["CALENDAR_DATE"] == date_obj]["WEEK_END_DATE"].iloc[0]
        date_obj -= timedelta(days=3)
        while current_week_end >= date_obj:
            last_trading_day = current_week_end
            current_week_end -= timedelta(days=7)
            current_week_end = df[df["CALENDAR_DATE"] == current_week_end][
                "WEEK_END_DATE"
            ].iloc[0]
            return current_week_end

    elif period == "month":
        current_month_end = df[df["CALENDAR_DATE"] == date_obj]["MONTH_END_DATE"].iloc[
            0
        ]
        date_obj -= timedelta(days=3)
        while current_month_end >= date_obj:
            last_trading_day = current_month_end
            current_month_end = datetime(
                current_month_end.year, current_month_end.month, 1
            ) - timedelta(days=10)
            current_month_end = current_month_end.date()
            current_month_end = df[df["CALENDAR_DATE"] == current_month_end][
                "MONTH_END_DATE"
            ].iloc[0]
            return current_month_end

    elif period == "quarter":
        current_quarter_end = df[df["CALENDAR_DATE"] == date_obj][
            "QUARTER_END_DATE"
        ].iloc[0]
        current_quarter_start = df[df["CALENDAR_DATE"] == date_obj][
            "QUARTER_START_DATE"
        ].iloc[0]
        date_obj -= timedelta(days=3)
        while current_quarter_end >= date_obj:
            last_trading_day = current_quarter_start
            current_quarter_start = datetime(
                current_quarter_start.year, current_quarter_start.month, 1
            ) - timedelta(days=10)
            current_quarter_end = current_quarter_start.date()
            current_quarter_end = df[df["CALENDAR_DATE"] == current_quarter_end][
                "QUARTER_END_DATE"
            ].iloc[0]
            return current_quarter_end

    elif period == "6months":
        six_months_ago = date_obj - timedelta(days=6 * 30)  # 简化计算
        six_months_end = df[df["CALENDAR_DATE"] == six_months_ago][
            "MONTH_END_DATE"
        ].iloc[0]
        return six_months_end

    elif period == "year":
        year_start_date = df[df["CALENDAR_DATE"] == date_obj]["YEAR_START_DATE"].iloc[0]
        return year_start_date


def calculate_financial_dates(df, date_str: str):
    """
    计算给定日期的前一个交易日、前一周的最后一个交易日、前一个月的最后一个交易日、前六个月的最后一个交易日以及年初以来的第一个交易日。

    参数：
    date_str: 日期字符串，格式为 YYYY-MM-DD

    返回：
    dict: 包含计算出的日期的字典
    """
    previous_trading_day = get_previous_trading_day(df, date_str)
    last_trading_day_of_week = get_last_trading_day_of_period(df, date_str, "week")
    last_trading_day_of_month = get_last_trading_day_of_period(df, date_str, "month")
    last_trading_day_of_quarter = get_last_trading_day_of_period(
        df, date_str, "quarter"
    )
    last_trading_day_of_6months = get_last_trading_day_of_period(
        df, date_str, "6months"
    )
    first_trading_day_of_year = get_last_trading_day_of_period(df, date_str, "year")

    financial_dates = {
        "previous_trading_day": previous_trading_day,
        "last_trading_day_of_week": last_trading_day_of_week,
        "last_trading_day_of_month": last_trading_day_of_month,
        "last_trading_day_of_quarter": last_trading_day_of_quarter,
        "last_trading_day_of_6months": last_trading_day_of_6months,
        "first_trading_day_of_year": first_trading_day_of_year,
    }

    return financial_dates


def get_trading_days(start: str, end: str) -> list:
    """
    获取交易日期并存储为列表

    参数:
    start: 日期字符串，格式为 YYYY-MM-DD
    end:   日期字符串，格式为 YYYY-MM-DD

    返回:
    list: 包含筛选出的区间内交易日期
    """
    start = datetime.strptime(start, "%Y-%m-%d")
    start = start.date()
    end = datetime.strptime(end, "%Y-%m-%d")
    end = end.date()

    df = Database.query_pd(
        "server93Api",
        f"SELECT calendarDate FROM trade_cal WHERE exchangeCD = 'XSHG' AND isOpen = 1  AND calendarDate BETWEEN '{start}' and '{end}' ",
    )
    tradedate_list = df["calendarDate"].tolist()
    return tradedate_list


def get_halt_stocks(date: str) -> list:
    """
    获取指定日期的停牌股票并存储为列表

    参数:
    date: 日期字符串，格式为 YYYY-MM-DD

    返回:
    list: 包含筛选出的停牌股票
    """
    date = datetime.strptime(date, "%Y-%m-%d")
    date = date.date()

    df = Database.query_pd(
        "server93Api", f"SELECT ticker,isOpen FROM mkt_equd WHERE tradeDate = '{date}'"
    )

    df_halt = df[df["isOpen"] == 0]
    halt_list = df_halt["ticker"].tolist()
    return halt_list


def get_st_stocks(date: str) -> list:
    """
    获取指定日期的ST股票并存储为列表

    参数:
    date: 日期字符串，格式为 YYYY-MM-DD

    返回:
    list: 包含筛选出的ST股票
    """
    date = datetime.strptime(date, "%Y-%m-%d")
    date = date.date()

    df = Database.query_pd(
        "server93Api", f"SELECT ticker FROM sec_st WHERE tradeDate = '{date}'"
    )
    st_list = df["ticker"].tolist()
    return st_list


def get_div_stocks(date: str) -> pd.DataFrame:
    """
    获取指定日期的股票分红信息并存储为DataFrame

    参数:
    date: 日期字符串，格式为 YYYY-MM-DD

    返回:
    DataFrame: 包含筛选出的分红股票数量信息和日期信息
    """

    df = Database.query_pd(
        "server93Api", f"SELECT * FROM equ_div WHERE exDivDate = '{date}'"
    )
    return df


# def Annual(indicator:str,moved_years:int):


def Less(*args):
    """
    返回所有指标中最小的一个

    参数:
    args: 存储所有指标的参数

    返回:
    min: 所有指标中的最小值
    """
    if not 1 <= len(args) <= 8:
        raise ValueError("参数数量必须在1到8之间")

    return min(args)


def Greater(*args):
    """
    返回所有指标中最大的一个

    参数:
    args: 存储所有指标的参数

    返回:
    min: 所有指标中的最大值
    """
    if not 1 <= len(args) <= 8:
        raise ValueError("参数数量必须在1到8之间")

    return max(args)


def get_last_day_of_year(year):
    """
    返回给定年份的最后一天

    参数:
    year: 年份

    返回:
    datetime: 给定年份的最后一天
    """
    return datetime(year, 12, 31)


def get_period_date(tag: str):
    """
    获取前N个交易日对应的日期
    """
    sql = f"""
        WITH ranked_dates AS (
            SELECT calendarDate,
                ROW_NUMBER() OVER (ORDER BY calendarDate DESC) AS row_num
            FROM  trade_cal
            WHERE isOpen = 1 AND exchangeCD = 'XSHE' AND calendarDate <= CURRENT_DATE
        )
        SELECT calendarDate
        FROM ranked_dates
        WHERE row_num <= {tag}
        ORDER BY calendarDate;
    """
    df = Database.query_pd("server93Api", sql)
    res = df.sort_values(by="calendarDate")
    return res
