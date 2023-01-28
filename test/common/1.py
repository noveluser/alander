#!/usr/bin/python
# coding=utf-8


# Use your own values here
api_id = 10917789
api_hash = '01a5ae88d6cfe432d809eb371c4c88e2'


# import asyncio
# import base64
# import json
# import os
# import re
# import requests
# from telethon import TelegramClient, events



# async def main():
#     MSG = 'test'
#     CHANNEL_ID = '@flywangle12TestBot'
#     async with TelegramClient("CLIENT_NAME", api_id, api_hash) as client:
#         await client.send_message(CHANNEL_ID, MSG)


import os
import time
from telethon import TelegramClient, events, sync

api_id = [10917789]	#输入api_id，一个账号一项
api_hash = ['01a5ae88d6cfe432d809eb371c4c88e2']	#输入api_hash，一个账号一项


session_name = "id_"
client = TelegramClient(session_name, 10917789, '01a5ae88d6cfe432d809eb371c4c88e2')
client.start()
client.send_message("@luxiaoxun_bot", '/checkin')	#第一项是机器人ID，第二项是发送的文字
time.sleep(5)	#延时5秒，等待机器人回应（一般是秒回应，但也有发生阻塞的可能）
client.send_read_acknowledge("@luxiaoxun_bot")	#将机器人回应设为已读
print("Done! Session name:", session_name[num])
	
os._exit(0)