#/bin/usr/bash

# Generate CA files ca.crt and servercakey.pem. This allows the signing of server and client keys:
openssl genrsa -out servercakey.pem
openssl req -new -x509 -key servercakey.pem -out ca.crt
#Create the server private key (server.crt) and public key (server.key):
openssl genrsa -out server.key
openssl req -new -key server.key -out server_reqout.txt
openssl x509 -req -in server_reqout.txt -days 3650 -sha256 -CAcreateserial -CA ca.crt -CAkey servercakey.pem -out server.crt
#Create the client private key (client.crt) and public key (client.key):
openssl genrsa -out client.key
openssl req -new -key client.key -out client_reqout.txt
openssl x509 -req -in client_reqout.txt -days 3650 -sha256 -CAcreateserial -CA ca.crt -CAkey servercakey.pem -out client.crt
# Delete unneeded files.
rm -rf ca.srl client_reqout.txt server_reqout.txt servercakey.pem
