# log.py - 项目根目录下的日志配置
import logging
import logging.config
import os
from datetime import datetime

# ====================== 日志配置 ======================
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            'format': '%(asctime)s - %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'file': {
            'format': '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'console',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'file',
            'filename': 'log/identifymcsbag.log',                 # 直接放在项目根目录
            'maxBytes': 10 * 1024 * 1024,          # 10MB 轮转
            'backupCount': 5,
            'encoding': 'utf-8'
        }
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console', 'file']
    }
}

def init_logger():
    """初始化日志系统（在程序入口调用一次即可）"""
    # 确保日志目录存在（如果使用子目录，可取消注释下面两行）
    log_dir = os.path.dirname(LOG_CONFIG['handlers']['file']['filename'])
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logging.config.dictConfig(LOG_CONFIG)

# 立即初始化（让其他模块导入后自动生效，适合非入口模块）
init_logger()