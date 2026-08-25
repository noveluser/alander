#!/usr/bin/python
# coding=utf-8
"""便捷启动入口：python run_reason_classifier.py [YYYYMMDD]

仅输出 reason_classification_<范围>.xlsx。
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from reason_classifier.main import main  # noqa: E402

if __name__ == "__main__":
    main()
