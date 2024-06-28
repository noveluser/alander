#!/usr/bin/python
# coding=utf-8

# 暂定
# wangle
# v0.2

import sys

poem = ["床前明月光", "疑是地上霜", "举头望明月", "低头思故乡"]

def display_poem():
  """根据用户输入的数字显示对应诗句，输入 0 退出。"""
  while True:
    print("请输入想要显示的诗句行数（1-4），输入 0 退出：")
    try:
      line_num = int(input())
      if line_num == 0:
        break
      elif 1 <= line_num <= len(poem):
        print(poem[line_num - 1])
      else:
        print("输入错误，请输入 1-4 之间的数字或 0 退出。")
    except ValueError:
      print("输入错误，请输入数字。")

if __name__ == "__main__":
  display_poem()
    
