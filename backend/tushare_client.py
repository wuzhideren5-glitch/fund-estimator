"""
Tushare 客户端初始化 — 使用自定义 endpoint
所有模块统一 import 此文件获取 pro 实例
"""
import tushare as ts

TOKEN = '34d3ae72bb74090061fc9e9009e47c48af1b5118644487064a169c5c'
ENDPOINT = 'http://101.35.233.113:8020/'


def get_pro():
    """获取 Tushare Pro API 实例"""
    pro = ts.pro_api(TOKEN)
    pro._DataApi__http_url = ENDPOINT
    return pro


def get_pro_bar(ts_code: str, **kwargs):
    """获取行情数据（使用 ts.pro_bar）"""
    pro = get_pro()
    return ts.pro_bar(api=pro, ts_code=ts_code, **kwargs)
