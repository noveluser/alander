#!/usr/bin/python
# coding=utf-8

import pandas as pd

# 创建一个示例DataFrame
date_a = "2024-06-02"
transfer_date = "{}-{}-{}".format(date_a[8:10],date_a[5:7],date_a[0:4])

print(transfer_date)
