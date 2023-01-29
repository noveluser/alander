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

a = [1,2,3,4,5]
times = int(len(a)/10) + 1
print(times)
for i in range(times):
    first = a[i*10:i+9]
    print(first)

