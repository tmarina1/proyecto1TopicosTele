from fastapi import FastAPI, responses, Request
import uvicorn
import grpc
import messages_pb2
import messages_pb2_grpc
import sys
from google.protobuf.json_format import MessageToDict
import json
import base64

#uvicorn apiGateway:app --reload
app = FastAPI()
round_robin = 0

f = open('config.json')
settings = json.load(f)
f.close()
SERVERS = settings['SERVERS']

def roundRobin():
  global round_robin
  global SERVERS
  if round_robin == len(SERVERS)-1:
    round_robin = 0
  else:
    round_robin += 1

  return SERVERS[round_robin]

def conexionBalanceada(request):
  servidor = roundRobin()
  try:
    conexionGRPC = gRPC(request, servidor)
  except:
    try:
      conexionGRPC = gRPCreplicacion(request)
      servidor = roundRobin()
      conexionGRPC = gRPC(request, servidor)
    except:
      return 'Todos los servidores estan fuera de servicio!'
  conexionGRPC = ''.join(conexionGRPC['results'])
  return conexionGRPC

@app.get("/", response_class=responses.PlainTextResponse)
def root():
  return "Para crear una cola digite /crearCola/'nombreCola' \nPara Listar una cola digite /listarColas"

@app.get("/crearCola/{nombreCola}")
def root(nombreCola):
  request = f'crearCola/{nombreCola}'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(response['results'])

  return {"Respuesta": response}

@app.get("/borrarCola/{nombreCola}")
def root(nombreCola):
  request = f'borrarCola/{nombreCola}'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/listarColas")
def root():
  request = 'listarColas'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/agregarElementoCola/{nombreCola}/{mensaje}")
def root(nombreCola, mensaje, request: Request):
  clienteHost = request.client.host
  request = f'agregarElementoCola/{nombreCola}/{mensaje}%{clienteHost}'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/verCola/{nombreCola}")
def root(nombreCola):
  request = f'verCola/{nombreCola}'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/consumir")
def root(request: Request):
  clienteHost = request.client.host
  request = f'consumir&{clienteHost}'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

#######################################

@app.get("/crearTopico/{nombreTopico}")
def root(nombreTopico):
  request = f'crearTopico/{nombreTopico}'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(conexionGRPC['results'])

  return {"Respuesta": response}

@app.get("/eliminarTopico/{nombreTopico}")
def root(nombreTopico):
  request = f'eliminarTopico/{nombreTopico}'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/agregarMensajeTopico/{nombreTopico}/{mensaje}")
def root(nombreTopico, mensaje, request: Request):
  clienteHost = request.client.host
  request = f'agregarMensajeTopico/{nombreTopico}/{mensaje}%{clienteHost}'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/verMensajesTopico/{nomreTopico}")
def root(nomreTopico):
  request = f'verMensajesTopico/{nomreTopico}'
  request = encriptar(request)
  response = conexionBalanceada(request)
  #response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

def gRPC(request, servidor):
  channel = grpc.insecure_channel(servidor)
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.message(messages_pb2.instructionRequest(query=request))
  response  = MessageToDict(response)
  return response 

def gRPCreplicacion(request):
  global settings
  mom2 = settings['SERVIDOR_SECUNDARIO']
  channel = grpc.insecure_channel(mom2)
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.message(messages_pb2.instructionRequest(query=request))
  response  = MessageToDict(response)
  return response 

def encriptar(texto):
  texto = texto.encode('utf-8') 
  texto = base64.b64encode(texto).decode('utf-8')
  return texto

if __name__ == "__main__":
  uvicorn.run(app, host="127.0.0.1", port=8001)