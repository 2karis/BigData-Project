import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours
CONSUMER_KEY = 'SaonwOZBo6804BNw8qDBVYlXY'
CONSUMER_SECRET = 'SnOEeE7ThjSeV28Uln0QGaDytSKkXkYv6G3bYzsxDxP0SHXgvq'
ACCESS_TOKEN = '4901913808-8ReS7xOkyyRsC3IJSEaZE9wN1ZubrHWmtLUYbfR'
ACCESS_SECRET = 'xZ7r94sz4IYsTl0cwOHpTOS99CCbNGrzpWsc8BnqvmWLH'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_data():
	url = 'https://api.twitter.com/1.1/search/tweets.json?q=coronavirus&count=50'
	response = requests.get(url, auth=my_auth, stream=True)
	#response = requests.get(url, stream=True)

	print(url, response)
	return response

def send_data_to_spark(response, tcp_connection):
	for line in response.iter_lines():
		full_data = json.loads(line.decode('utf-8','ignore'))
		data_text = full_data['statuses']
		for data in data_text:
			tweet = str(data["text"].encode('utf-8'))
			print(str(data["text"].encode('utf-8')))
			tcp_connection.sendall(tweet.encode('utf-8'))


TCP_IP = "localhost"
TCP_PORT = 9009
while 1:
	conn = None
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((TCP_IP, TCP_PORT))
	s.listen(10)
	print("Waiting for TCP connection...")
	conn, addr = s.accept()
	print("Connected... Starting getting data.")
	resp = get_data()
	send_data_to_spark(resp, conn)