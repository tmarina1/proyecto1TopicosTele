import os
import grpc
import messages_pb2
import messages_pb2_grpc
import uvicorn
from fastapi import FastAPI, responses, Request
from google.protobuf.json_format import MessageToDict
import base64
import json

app = FastAPI()
round_robin = 0

f = open('config.json')
settings = json.load(f)
f.close()
SERVERS = settings['SERVERS']
falloMOM = False

def roundRobin():
  global round_robin
  global SERVERS
  if round_robin == len(SERVERS)-1:
    round_robin = 0
  else:
    round_robin += 1
  print(SERVERS[round_robin])
  return SERVERS[round_robin]

def conexionBalanceada(request, tipoDeRespuesta):
  servidor = roundRobin()
  global falloMOM
  try:
    if falloMOM:
      comprobar = 'estaVivo'
      comprobar = encriptar(comprobar)
      comprobacion = gRPC(comprobar, tipoDeRespuesta, servidor)
      falloMOM = False
      servidor = roundRobin()
      conexionGRPC = gRPC(request, tipoDeRespuesta, servidor)
    else:
      conexionGRPC = gRPC(request, tipoDeRespuesta, servidor)
  except:
    try:
      falloMOM = True
      #conexionGRPC = gRPCreplicacion(request)
      servidor = roundRobin()
      conexionGRPC = gRPC(request, tipoDeRespuesta, servidor)
    except:
      return 'Todos los servidores estan fuera de servicio!'
  conexionGRPC = ''.join(conexionGRPC['results'])
  return conexionGRPC

@app.get("/consumirCola/{nombreCola}")
def root(nombreCola):
  response = conexionCola(nombreCola)

  return {"Respuesta": response}

@app.get("/consumirTopico/{nombreTopico}/{nombreSuscriptor}")
def root(nombreTopico, nombreSuscriptor):
  response = conexionTopico(nombreTopico, nombreSuscriptor)

  return {"Respuesta": response}

@app.get("/suscribirse/{nombreTopico}/{nombreSuscriptor}")
def root(nombreTopico, nombreSuscriptor):
  response = suscribirse(nombreTopico, nombreSuscriptor)

  return {"Respuesta": response}

def conexionCola(nombreCola):
  request = f'cCola/{nombreCola}'
  request = encriptar(request)
  peticion = conexionBalanceada(request, False)
  error = peticion
  val = peticion
  if 'listarArchivos' in val:
    ip = val.split('%')[1]
    listar = listarArchivos()
    respuesta = f'{str(listar)}&{ip}'
    respuesta = encriptar(respuesta)
    respuesta = conexionBalanceada(respuesta, True)
    return respuesta
  elif 'buscarArchivo' in val:
    ip = val.split('%')[1]
    nombreArchivo = val[val.index('&')+1:val.index('%')]
    listar = buscarArchivo(nombreArchivo)
    respuesta = f'{str(listar)}&{ip}'
    respuesta = encriptar(respuesta)
    respuesta = conexionBalanceada(respuesta, True)
    return respuesta
  return error

def suscribirse(nombreTopico, nombreSuscriptor):
  request = f'suscribirTopico/{nombreTopico}/{nombreSuscriptor}'
  request = encriptar(request)
  respuesta = conexionBalanceada(request, False)
  return respuesta

def conexionTopico(nombreTopico, nombreSuscriptor):
  request = f'conTopico/{nombreTopico}/{nombreSuscriptor}'
  request = encriptar(request)
  peticion = conexionBalanceada(request, False)
  error = peticion
  val = peticion
  print(peticion)
  if 'listarArchivos' in val:
    ip = val.split('%')[1]
    listar = listarArchivos()
    respuesta = f'{str(listar)}&{ip}'
    respuesta = encriptar(respuesta)
    respuesta = conexionBalanceada(respuesta, True)
    return respuesta
  elif 'buscarArchivo' in val:
    ip = val.split('%')[1]
    nombreArchivo = val[val.index('&')+1:val.index('%')]
    listar = buscarArchivo(nombreArchivo)
    respuesta = f'{str(listar)}&{ip}'
    respuesta = encriptar(respuesta)
    respuesta = conexionBalanceada(respuesta, True)
    return respuesta
  return error

def listarArchivos():
  listaDirectorios = os.listdir('/')
  return listaDirectorios

def buscarArchivo(nombreArchivo):
  isFound = 'Archivo no existe'
  p = os.path.join(os.path.expanduser("~"), "Documents")
  for root, dirs, files in os.walk(p):
      if nombreArchivo in files:
          isFound = 'Existe!'
  return isFound

def gRPC(request, tipoDeRetorno, servidor):
  channel = grpc.insecure_channel(servidor)
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.message(messages_pb2.instructionRequest(query=request, respuesta=tipoDeRetorno))
  response  = MessageToDict(response)
  return response 

def encriptar(texto):
  texto = texto.encode('utf-8') 
  texto = base64.b64encode(texto).decode('utf-8')
  return texto

if __name__ == '__main__':
  uvicorn.run(app, host="127.0.0.1", port=8002)
