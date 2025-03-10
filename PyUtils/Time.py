import time
import datetime
from functools import wraps


def timeit(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        t0_ = time.time()
        ret = func(*args, **kwargs)
        print("%s cost %.2f secs." % (func.__name__, time.time() - t0_))
        return ret

    return wrapper


def transfor2secondes(time_str="0940"):
    """把时间准换为秒的形式，方便计算时间的距离"""
    time_str = str(time_str)
    return int(time_str[:-2]) * 3600 + int(time_str[-2:]) * 60


def trans_hour_2_kf(hour_str="0930", day=None):
    """把配置中的小时分钟转换为卡方母单的格式：
    输入'0930',输出为"20240920093000000"
    """
    if isinstance(hour_str, (int, float)):
        hour_str = str(int(hour_str))
    hour_str = hour_str.zfill(4)
    if day is None:
        day = datetime.date.today().strftime("%Y%m%d")
    return day + hour_str + "00000"


def retry(func):
    max_retries = 3
    delay = 1

    @wraps(func)
    def wrapper(*args, **kwargs):
        for i in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"{func.__name__} 异常: {e}")
                if i < max_retries:
                    time.sleep(delay * (2**i))  # 指数退避
                else:
                    raise

    return wrapper
