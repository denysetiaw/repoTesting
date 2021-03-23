import requests
import json
import telepot
from telepot.loop import MessageLoop
import time,datetime
from bs4 import BeautifulSoup

url_nifi = [{'http://192.168.24.151:8080/nifi-api/process-groups/378cb18c-0178-1000-0000-000043210b67'},
	   {'http://192.168.24.151:8080/nifi-api/process-groups/1caa6205-9191-34f1-840e-bb07b2f51605'},
			{'http://192.168.24.151:8080/nifi-api/process-groups/1f796bcc-7168-3db4-a9b8-e6711ee932b3'},
			# {'http://192.168.24.151:8080/nifi-api/flow/process-groups/37b0662e-0178-1000-0000-0000034f4703'},
			{'http://192.168.24.151:8080/nifi-api/process-groups/1ceb1480-404b-3014-9ad7-da4b43bfe570'},
			{'http://192.168.24.151:8080/nifi-api/process-groups/c269c21b-3a3a-3aff-b5a3-2501a90e8ce9'},
			{'http://192.168.24.151:8080/nifi-api/flow/process-groups/37b0662e-0178-1000-0000-0000034f4703'},
			{'http://192.168.24.151:8080/nifi-api/process-groups/1adb46db-6561-3dee-be3d-030ca87639e4'},
			{'http://192.168.24.151:8080/nifi-api/process-groups/c5285f5f-702b-3f10-b6cb-d0456efea14c'}]
url_kafka = [{'IMA online news':'http://192.168.24.21:9000/clusters/IPD-Production/consumers/fix-ima-news/topic/''ann-online-news/type/KF'},
			{'IMA online news':'http://192.168.24.21:9000/clusters/ClusterBT/consumers/split-statement-ima-1/topic/''split-statement-ima/type/KF'},
			{'IMA tv news':'http://192.168.24.21:9000/clusters/IPD-Production/consumers/ann-tv-news/topic/''ann-tv-news/type/KF'},
			{'IMA printed news':'http://192.168.24.21:9000/clusters/IPD-Production/consumers/ann-printed-news/topic/''ann-printed-news/type/KF'},
			{'IMA reprocess online news':'http://192.168.24.21:9000/clusters/IPD-Production/consumers/ima-online-reprocess-new-flow/topic/''ann-online-news-reprocess/type/KF'},
			{'IMA reprocess online statement':'http://192.168.24.21:9000/clusters/ClusterBT/consumers/split-statement-ima-reprocess-newtest/topic/''split-statement-ima-reprocess/type/KF'},
			{'IMA twitter':'http://192.168.24.21:9000/clusters/IPD-Production/consumers/ima-twitter-ingest/topic/''ima-twitter-ingest/type/KF'},
			{'IMA facebook post':'http://192.168.24.21:9000/clusters/IPD-Production/consumers/ima-fb-post-ingest-2/topic/''ima-fb-post-ingest/type/KF'},
			{'IMA facebook comment':'http://192.168.24.21:9000/clusters/IPD-Production/consumers/ima-fb-comment-ingest-2/topic/''ima-fb-comment-ingest/type/KF'}]

# def action(msg):
#     chat_id = msg['chat']['id']
#     command = msg['text']
#     print ('Received: %s' % command)
#     if command == '/check':
#         telegram_bot.sendMessage (chat_id, str(antrian()+kafka_lag()))
# 	elif command == '/kafkalag':
# 		telegram_bot.sendMessage(chat_id, str(antrian() + kafka_lag()))
#     elif command == '/help':
#         telegram_bot.sendMessage(chat_id, str("untuk menjalankan harus dengan perintah '/check',"
# 											  "jika eroor please contact @deny_setiawan"))
# 	# elif command == '/helpa':
# 	# telegram_bot.sendMessage(chat_id, str("untuk menjalankan harus dengan perintah '/check'"))

def gas(msg):
	chat_id = msg['chat']['id']
	command = msg['text']
	print(command)
	if command == '/nifiLag':
		telegram_bot.sendMessage(chat_id, str(antrian()))
	elif command == '/kafkaLag':
		telegram_bot.sendMessage(chat_id, str(kafka_lag()))
	elif command == '/help':
		telegram_bot.sendMessage(chat_id, str("untuk menjalankan nifi queue harus dengan perintah '/nifiLag',\n"
											  "untuk menjalankan kafka harus dengan perintah '/kafkaLag',\n"
											  "jika error please contact @deny_setiawan"))

def antrian():
	message = "Nifi Queued:\n\n"
	for i in url_nifi:
		# print(i)
		for v in i:
			bot = requests.	get(v)

			a = bot.text
			b = json.loads(a)
			if 'status' in b:
				name = b['status']['aggregateSnapshot']['name']
				que = b['status']['aggregateSnapshot']['queuedCount']
				size = b['status']['aggregateSnapshot']['queuedSize']
			else:
				online = b['processGroupFlow']['flow']['processGroups'][0]
				name = b['processGroupFlow']['breadcrumb']['breadcrumb']['name']
				onlineCount = online['status']['aggregateSnapshot']['queuedCount']
				onlineSize = online['status']['aggregateSnapshot']['queuedSize']
				statement = b['processGroupFlow']['flow']['processGroups']
				statementCount = online['status']['aggregateSnapshot']['queuedCount']
				statementSize = b['processGroupFlow']['flow']['processGroups']
				que = onlineCount
				size = onlineSize



			message += "{}\n".format(name +' :\n'+'Queued : '+que+'\nSize : '+size )+"\n"
			# print(message)

	return message
def kafka_lag():
    message = "Kafka Production Lag:\n"
    for kafka_link in url_kafka:
        # print(kafka_link)
        for k, v in kafka_link.items():
            req = requests.get(v)

            soup = BeautifulSoup(req.text)
            antrian = soup.find('table', class_='table')
            first_td = antrian.find('td').find_next_sibling('td')
            # print(first_td)
            for i in first_td:
                message += "{} = {}\n".format(k, i)

    return message


#perubahab

if __name__ == '__main__':
	gas
	telegram_bot = telepot.Bot('1063696970:AAEy7a_LcMRkmSOz0OYcqQEwGlfI5NK5SM4')
	# print(telegram_bot.getMe())
	MessageLoop(telegram_bot, gas).run_as_thread()
	print('Up and Running....')
	while 1:
		time.sleep(10)
