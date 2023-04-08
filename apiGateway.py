from fastapi import FastAPI, responses, Request
import uvicorn
import grpc
import messages_pb2
import messages_pb2_grpc
import sys
from google.protobuf.json_format import MessageToDict
import json

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

  return conexionGRPC

@app.get("/", response_class=responses.PlainTextResponse)
def root():
  return "Para crear una cola digite /crearCola/'nombreCola' \nPara Listar una cola digite /listarColas"

@app.get("/crearCola/{nombreCola}")
def root(nombreCola):
  request = f'crearCola/{nombreCola}'
  response = conexionBalanceada(request)
  response = ''.join(response['results'])

  return {"Respuesta": response}

@app.get("/borrarCola/{nombreCola}")
def root(nombreCola):
  request = f'borrarCola/{nombreCola}'
  conexionGRPC = gRPC(request)
  response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/listarColas")
def root():
  request = 'listarColas'
  conexionGRPC = gRPC(request)
  response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/agregarElementoCola/{nombreCola}/{mensaje}")
def root(nombreCola, mensaje, request: Request):
  clienteHost = request.client.host
  request = f'agregarElementoCola/{nombreCola}/{mensaje}%{clienteHost}'
  conexionGRPC = gRPC(request)
  response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/verCola/{nombreCola}")
def root(nombreCola):
  request = f'verCola/{nombreCola}'
  conexionGRPC = gRPC(request)
  response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/consumir")
def root(request: Request):
  clienteHost = request.client.host
  request = f'consumir&{clienteHost}'
  conexionGRPC = gRPC(request)
  response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

#######################################

@app.get("/crearTopico/{nombreTopico}")
def root(nombreTopico):
  request = f'crearTopico/{nombreTopico}'
  conexionGRPC = gRPC(request)
  response = ''.join(conexionGRPC['results'])

  return {"Respuesta": response}

@app.get("/eliminarTopico/{nombreTopico}")
def root(nombreTopico):
  request = f'eliminarTopico/{nombreTopico}'
  conexionGRPC = gRPC(request)
  response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/agregarMensajeTopico/{nombreTopico}/{mensaje}")
def root(nombreTopico, mensaje, request: Request):
  clienteHost = request.client.host
  request = f'agregarMensajeTopico/{nombreTopico}/{mensaje}%{clienteHost}'
  conexionGRPC = gRPC(request)
  response = ''.join(conexionGRPC['results'])
  
  return {"Respuesta": response}

@app.get("/verMensajesTopico/{nomreTopico}")
def root(nomreTopico):
  request = f'verMensajesTopico/{nomreTopico}'
  conexionGRPC = gRPC(request)
  response = ''.join(conexionGRPC['results'])
  
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

if __name__ == "__main__":
  uvicorn.run(app, host="127.0.0.1", port=8001)