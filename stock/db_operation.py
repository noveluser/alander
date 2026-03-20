#!/usr/bin/python3
# coding=utf-8
"""
数据库通用操作模块 - 收敛所有DB相关代码，提供通用化DB操作接口
特性：自动重试+批量更新+逐行兜底+异常隔离，主程序直接调用无需关注DB细节
"""
import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text
import sqlalchemy.exc

# 全局数据库引擎
db_engine = None

def create_db_engine(DB_URL, DB_CONFIG):
    """
    初始化数据库引擎（带连接池配置）
    :param DB_URL: 数据库连接地址
    :param DB_CONFIG: 连接池配置（pool_size/max_overflow/pool_recycle）
    """
    global db_engine
    try:
        db_engine = create_engine(
            DB_URL,
            pool_size=DB_CONFIG.get("pool_size", 10),
            max_overflow=DB_CONFIG.get("max_overflow", 20),
            pool_recycle=DB_CONFIG.get("pool_recycle", 3600),
            echo=DB_CONFIG.get("echo", False)
        )
        # 测试连接
        with db_engine.connect() as conn:
            pass
        logging.info("✅ 数据库引擎初始化成功，连接池配置生效")
    except Exception as e:
        logging.error(f"❌ 数据库引擎初始化失败: {str(e)}", exc_info=True)
        raise e

def safe_db_operation(func, *args, **kwargs):
    """
    数据库安全操作封装：自动重试+异常捕获，通用化装饰器式调用
    :param func: 数据库操作函数
    :param args/kwargs: 函数入参
    :return: 函数执行结果/None
    """
    retry_times = kwargs.pop("retry_times", 2)
    retry_interval = kwargs.pop("retry_interval", 0.5)
    for retry in range(retry_times + 1):
        try:
            return func(*args, **kwargs)
        except sqlalchemy.exc.SQLAlchemyError as e:
            if retry == retry_times:
                logging.error(f"❌ 数据库操作重试{retry_times}次后失败: {str(e)}", exc_info=True)
                return None
            logging.warning(f"⚠️  数据库操作第{retry+1}次失败（SQLAlchemyError），{retry_interval}秒后重试: {str(e)[:100]}")
            time.sleep(retry_interval)
        except Exception as e:
            logging.error(f"❌ 数据库操作非连接异常: {str(e)}", exc_info=True)
            return None

def db_read(sql, params=None):
    """
    通用数据库查询：返回DataFrame
    :param sql: 查询SQL
    :param params: SQL参数（字典）
    :return: pd.DataFrame/None
    """
    if db_engine is None:
        logging.error("❌ 数据库引擎未初始化，无法执行查询")
        return None
    try:
        with db_engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)
        logging.debug(f"ℹ️  数据库查询成功，返回{len(df)}条数据")
        return df
    except Exception as e:
        logging.error(f"❌ 数据库查询失败: {str(e)}", exc_info=True)
        raise e

def db_write(sql, params=None):
    """
    通用数据库写入/更新/删除：单条操作
    :param sql: 执行SQL
    :param params: SQL参数（字典）
    :return: 受影响行数/None
    """
    if db_engine is None:
        logging.error("❌ 数据库引擎未初始化，无法执行写入")
        return None
    try:
        with db_engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            conn.commit()
        logging.debug(f"ℹ️  数据库执行成功，受影响行数: {result.rowcount}")
        return result.rowcount
    except Exception as e:
        logging.error(f"❌ 数据库写入失败: {str(e)}", exc_info=True)
        raise e

def batch_update(sql, df):
    """
    批量更新数据库：基于DataFrame批量执行更新，适配SQLAlchemy
    :param sql: 批量更新SQL（带命名参数，与DF列名一致）
    :param df: 待更新的DataFrame（列名与SQL参数一一对应）
    重点！！！这里的参数用的很精妙，将DF列名和参数联系在一起，解决了传参的问题
    :return: 总受影响行数/0
    """
    if db_engine is None or df is None or df.empty:
        return 0
    total_affected = 0
    try:
        with db_engine.connect() as conn:
            for _, row in df.iterrows():
                params = row.to_dict()
                result = conn.execute(text(sql), params)
                total_affected += result.rowcount
            conn.commit()
        logging.info(f"✅ 数据库批量更新完成，总受影响行数: {total_affected}")
        return total_affected
    except Exception as e:
        logging.error(f"❌ 数据库批量更新失败: {str(e)}", exc_info=True)
        return 0

def retry_row_update(symbol, sql, df):
    """
    逐行重试更新：批量更新失败后，对单股票逐行兜底更新
    :param symbol: 股票代码（仅日志用）
    :param sql: 更新SQL
    :param df: 单股票待更新DataFrame
    :return: 成功更新行数/0
    """
    if df is None or df.empty:
        return 0
    
    success_rows = 0  # 记录成功行数（而非总受影响行数）
    for _, row in df.iterrows():
        params = row.to_dict()
        # 单条执行，单独捕获异常
        try:
            # 调用单条写操作，而非批量
            affected = safe_db_operation(db_write, sql, params=params)  
            if affected and affected > 0:
                success_rows += 1
            else:
                logging.warning(f"⚠️ {symbol} 单条更新无影响行数，参数：{params}")
        except Exception as e:
            logging.error(f"❌ {symbol} 单条更新失败，参数：{params}，错误：{str(e)}")
            # 单条失败不中断，继续下一行
    
    logging.info(f"✅ {symbol} 逐行重试更新完成，成功更新{success_rows}条（总计{len(df)}条）")
    return success_rows

# #!/usr/bin/python3
# # coding=utf-8
# """
# 通用数据库操作模块（无业务逻辑、无依赖缺失、修复所有报错）
# """
# import pandas as pd
# from sqlalchemy import create_engine, text
# from sqlalchemy.exc import SQLAlchemyError
# import logging

# # ====================== 全局变量（极简）======================
# _DB_ENGINE = None
# logger = logging.getLogger(__name__)

# # ====================== 创建引擎 ======================
# def create_db_engine(db_url, db_config=None):
#     """
#     初始化数据库引擎（只接收 URL + 连接池配置）
#     👉 修改点：不再接收 host/user/password，只认 URL
#     """
#     global _DB_ENGINE
#     if _DB_ENGINE is not None:
#         return _DB_ENGINE

#     default_config = {
#         "pool_size": 5,
#         "max_overflow": 10,
#         "pool_recycle": 3600,
#         "pool_pre_ping": True
#     }

#     if db_config and isinstance(db_config, dict):
#         default_config.update(db_config)

#     try:
#         _DB_ENGINE = create_engine(db_url, **default_config)
#         # 测试连接
#         with _DB_ENGINE.connect() as conn:
#             conn.execute(text("SELECT 1"))
#         logger.info("✅ 数据库引擎初始化成功")
#     except Exception as e:
#         logger.critical(f"❌ 数据库连接失败: {e}")
#         raise SystemExit(1)
#     return _DB_ENGINE

# # ====================== 读操作 ======================
# def db_read(sql, params=None):
#     """
#     通用查询（SELECT专用）
#     👉 修改点：必须用 text() 包裹 SQL，否则 :xxx 参数报错
#     """
#     if not _DB_ENGINE:
#         logger.error("❌ 引擎未初始化")
#         return pd.DataFrame()

#     try:
#         with _DB_ENGINE.connect() as conn:
#             df = pd.read_sql(text(sql), conn, params=params or {})
#         return df
#     except Exception as e:
#         logger.error(f"❌ 查询失败: {e}")
#         return pd.DataFrame()

# # ====================== 写操作（execute）======================
# def db_execute(sql, params=None):
#     """
#     通用写操作（INSERT/UPDATE/DELETE）
#     👉 修改点：自动用 text() 包裹 SQL + 自动事务提交/回滚
#     """
#     if not _DB_ENGINE:
#         logger.error("❌ 引擎未初始化")
#         return 0

#     if params is None:
#         return 0

#     # 统一参数格式
#     if isinstance(params, dict):
#         params = [params]

#     total = 0
#     try:
#         with _DB_ENGINE.connect() as conn:
#             try:
#                 result = conn.execute(text(sql), params)
#                 conn.commit()
#                 total = result.rowcount
#             except Exception as e:
#                 conn.rollback()
#                 raise e
#     except Exception as e:
#         logger.error(f"❌ 执行失败: {e}")
#     return total

# # ====================== 语义别名：读写分离 ======================
# def db_write(sql, params=None):
#     """
#     写操作专用接口（和 db_read 对称）
#     👉 修改点：只是别名，不重复写逻辑
#     """
#     return db_execute(sql, params)

# # ====================== 通用批量更新 ======================
# def batch_update(sql, df):
#     """
#     通用批量更新（无任何财务业务逻辑）
#     👉 修改点：只做 DF → 参数列表转换，不处理业务字段
#     """
#     if df is None or df.empty:
#         return 0

#     # 处理 NaN → None（数据库兼容）
#     params = df.to_dict("records")
#     params = [
#         {k: None if pd.isna(v) else v for k, v in row.items()}
#         for row in params
#     ]
#     return db_execute(sql, params)

# # ====================== 通用逐行重试 ======================
# def retry_row_update(stock_code, sql, df):
#     """
#     通用逐行更新（无业务逻辑）
#     👉 修改点：只做循环执行，不处理任何财务规则
#     """
#     if df is None or df.empty:
#         return 0

#     success = 0
#     for _, row in df.iterrows():
#         param = {k: None if pd.isna(v) else v for k, v in row.items()}
#         try:
#             cnt = db_execute(sql, param)
#             if cnt > 0:
#                 success += 1
#         except Exception as e:
#             logger.warning(f"⚠️ {stock_code} 行更新失败: {e}")
#     return success


# #!/usr/bin/python3
# # coding=utf-8
# """
# 标准化数据库操作模块 - 统一DB输入/输出接口
# 核心：SQLAlchemy单例引擎 + 通用SQL执行 + 批量更新/逐行重试
# """
# from sqlalchemy import create_engine, text
# from sqlalchemy.exc import SQLAlchemyError
# from sqlalchemy.pool import NullPool
# import pandas as pd
# import logging

# # 全局变量：SQLAlchemy引擎单例
# DB_ENGINE = None
# # 日志器（由utils初始化，此处直接获取）
# logger = logging.getLogger()

# # ---------------------- 引擎单例创建（通用，无业务耦合） ----------------------
# def create_db_engine(DB_URL, DB_CONFIG):
#     """创建SQLAlchemy引擎单例，全程复用连接池"""
#     global DB_ENGINE
#     if DB_ENGINE is not None:
#         return DB_ENGINE
#     try:
#         DB_ENGINE = create_engine(
#             DB_URL,
#             pool_size=DB_CONFIG["pool_size"],
#             max_overflow=DB_CONFIG["max_overflow"],
#             pool_recycle=DB_CONFIG["pool_recycle"],
#             pool_pre_ping=DB_CONFIG["pool_pre_ping"]
#         )
#         # 测试引擎有效性
#         with DB_ENGINE.connect() as conn:
#             conn.execute(text("SELECT 1"))
#         logger.info("✅ SQLAlchemy数据库引擎创建成功，连接池初始化完成")
#         return DB_ENGINE
#     except SQLAlchemyError as e:
#         logger.critical(f"❌ SQLAlchemy引擎创建失败: {str(e)}", exc_info=True)
#         raise SystemExit(1)
#     except Exception as e:
#         logger.critical(f"❌ 数据库连接异常: {str(e)}", exc_info=True)
#         raise SystemExit(1)

# # ---------------------- 标准化DB输入接口（读数据）：SQL→DF ----------------------
# def db_read(sql, params=None, df=None):
#     """
#     通用数据库读操作（数据输入），统一接口参数
#     :param sql: SQL查询语句（str/text）
#     :param params: SQL注入参数（dict，防注入），默认None
#     :param df: 预留参数，适配统一接口规范，默认None
#     :return: pd.DataFrame 查询结果（空数据返回空DF）
#     """
#     if DB_ENGINE is None:
#         logger.error("❌ 数据库引擎未初始化，无法执行读操作")
#         return pd.DataFrame()
#     try:
#         df_result = pd.read_sql(text(sql), DB_ENGINE, params=params or {})
#         logger.debug(f"✅ SQL读操作完成，返回{len(df_result)}条数据（DEBUG级）")
#         return df_result
#     except SQLAlchemyError as e:
#         logger.error(f"❌ SQL读操作失败: {str(e)}，SQL：{sql[:200]}...", exc_info=True)
#         return pd.DataFrame()
#     except Exception as e:
#         logger.error(f"❌ 数据库读操作异常: {str(e)}", exc_info=True)
#         return pd.DataFrame()


# def db_write(sql, params=None, df=None):
#     """
#     数据库写操作专用接口（语义封装，底层复用db_execute）
#     :param sql: SQL增删改语句（str/text）
#     :param params: SQL注入参数（dict/list[dict]，批量传列表），默认None
#     :param df: 可选，DF结构数据（批量更新时用，优先级高于params），默认None
#     :return: int 影响行数
#     """
#     # 复用db_execute的核心逻辑，仅做语义封装
#     return db_execute(sql, params=params, df=df)

# # ---------------------- 标准化DB输出接口（写数据）：SQL/DF→数据库 ----------------------
# def db_execute(sql, params=None, df=None):
#     """
#     通用数据库写操作（数据输出），统一接口参数，支持单条/批量
#     :param sql: SQL增删改语句（str/text）
#     :param params: SQL注入参数（dict/list[dict]，批量传列表），默认None
#     :param df: 可选，DF结构数据（批量更新时用，优先级高于params），默认None
#     :return: int 影响行数
#     """
#     if DB_ENGINE is None:
#         logger.error("❌ 数据库引擎未初始化，无法执行写操作")
#         return 0
#     # 若传入DF，优先将DF转为dict列表（适配批量更新）
#     if df is not None and not df.empty:
#         params = df.to_dict(orient="records")
#         # 核心修复：将所有 NaN 替换为 None，兼容 MySQL
#         params = [{k: (None if pd.isna(v) else v) for k, v in row.items()} for row in params]
        
#     if params is None or len(params) == 0:
#         logger.warning("⚠️  无有效参数，跳过DB写操作")
#         return 0

#     try:
#         with DB_ENGINE.connect() as conn:
#             result = conn.execute(text(sql), params)
#             conn.commit()
#         affected_rows = result.rowcount
#         logger.debug(f"✅ SQL写操作完成，影响{affected_rows}条数据（DEBUG级）")
#         return affected_rows
#     except SQLAlchemyError as e:
#         logger.error(f"❌ SQL写操作失败: {str(e)}，SQL：{sql[:200]}...", exc_info=True)
#         return 0
#     except Exception as e:
#         logger.error(f"❌ 数据库写操作异常: {str(e)}", exc_info=True)
#         return 0

# # ---------------------- 批量更新（业务无关的DB封装，适配财务数据更新） ----------------------
# def batch_update(sql, params=None, df=None):
#     """
#     批量更新封装（基于db_execute），适配财务数据的批量更新场景
#     :param sql: 批量更新SQL语句
#     :param params: 注入参数，默认None
#     :param df: 待更新的DF结构数据（核心）
#     :return: int 影响行数
#     """
#     if df is None or df.empty:
#         logger.warning("⚠️  无有效DF数据，跳过批量更新")
#         return 0
#     # 数据清洗：过滤主键空值、TTM值全0的行
#     df_clean = df.copy()
#     df_clean = df_clean[
#         (df_clean["secucode"].notna()) & 
#         (df_clean["report_time"].notna()) & 
#         ((df_clean["ttm_revenue"] != 0) | (df_clean["ttm_netprofit"] != 0))
#     ]
#     # 日期统一转为date类型（匹配数据库）
#     df_clean["report_time"] = df_clean["report_time"].dt.date
#     logger.info(f"📌 批量更新数据清洗完成，有效数据{len(df_clean)}条（原{len(df)}条）")
#     # 调用标准化写接口
#     return db_execute(sql, params=params, df=df_clean)

# # ---------------------- 逐行重试更新（兜底方案，基于db_execute） ----------------------
# def retry_row_update(stock_code, sql, params=None, df=None):
#     """
#     逐行重试更新（批量更新失败时的兜底），适配财务数据
#     :param stock_code: 股票代码（日志用）
#     :param sql: 逐行更新SQL语句
#     :param params: 注入参数，默认None
#     :param df: 待更新的DF结构数据（核心）
#     :return: int 成功更新行数
#     """
#     if df is None or df.empty:
#         logger.warning(f"⚠️ {stock_code} 无有效DF数据，跳过重试更新")
#         return 0
#     success_count = 0
#     # 逐行处理DF
#     for _, row in df.iterrows():
#         # 单行车参数构造
#         row_param = {
#             "secucode": str(row["secucode"]).split('.')[0].strip(),
#             "report_time": row["report_time"].date() if not pd.isna(row["report_time"]) else None,
#             "ttm_revenue": row["ttm_revenue"] if pd.notna(row["ttm_revenue"]) else 0,
#             "ttm_netprofit": row["ttm_netprofit"] if pd.notna(row["ttm_netprofit"]) else 0
#         }
#         # 过滤主键空值
#         if not row_param["secucode"] or pd.isna(row_param["report_time"]):
#             continue
#         # 调用标准化写接口
#         if db_execute(sql, params=row_param) > 0:
#             success_count += 1
#         else:
#             logger.warning(f"⚠️ {stock_code} 报告期{row_param['report_time']} 单条更新失败")
#     logger.info(f"🔄 {stock_code}逐行重试完成，成功{success_count}条")
#     return success_count