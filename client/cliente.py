import os
import grpc
import messages_pb2
import messages_pb2_grpc
import uvicorn
from fastapi import FastAPI, responses, Request
from google.protobuf.json_format import MessageToDict
import base64

app = FastAPI()

@app.get("/consumirCola/{nombreCola}")
def root(nombreCola):
  response = conexionCola(nombreCola)
  response = ''.join(response['results'])

  return {"Respuesta": response}

@app.get("/consumirTopico/{nombreTopico}/{nombreSuscriptor}")
def root(nombreTopico, nombreSuscriptor):
  response = conexionTopico(nombreTopico, nombreSuscriptor)
  response = ''.join(response['results'])

  return {"Respuesta": response}

@app.get("/suscribirse/{nombreTopico}/{nombreSuscriptor}")
def root(nombreTopico, nombreSuscriptor):
  response = suscribirse(nombreTopico, nombreSuscriptor)
  response = ''.join(response['results'])

  return {"Respuesta": response}

def conexionCola(nombreCola):
  request = f'cCola/{nombreCola}'
  request = encriptar(request)
  peticion = gRPC(request)
  error = peticion
  val = ''.join(peticion['results'])
  if 'listarArchivos' in val:
    ip = val.split('%')[1]
    listar = listarArchivos()
    respuesta = f'{str(listar)}&{ip}'
    respuesta = encriptar(respuesta)
    respuesta = gRPCrespuesta(respuesta, True)
    return respuesta
  elif 'buscarArchivo' in val:
    ip = val.split('%')[1]
    nombreArchivo = val[val.index('&')+1:val.index('%')]
    listar = buscarArchivo(nombreArchivo)
    respuesta = f'{str(listar)}&{ip}'
    respuesta = encriptar(respuesta)
    respuesta = gRPCrespuesta(respuesta, True)
    return respuesta
  return error

def suscribirse(nombreTopico, nombreSuscriptor):
  request = f'suscribirTopico/{nombreTopico}/{nombreSuscriptor}'
  request = encriptar(request)
  respuesta = gRPC(request)
  return respuesta

def conexionTopico(nombreTopico, nombreSuscriptor):
  request = f'conTopico/{nombreTopico}/{nombreSuscriptor}'
  request = encriptar(request)
  peticion = gRPC(request)
  error = peticion
  print(peticion)
  val = ''
  try: 
    val = ''.join(peticion['results'])
  except:
    error = 'no existe usuario'
  if 'listarArchivos' in val:
    ip = val.split('%')[1]
    listar = listarArchivos()
    respuesta = f'{str(listar)}&{ip}'
    respuesta = encriptar(respuesta)
    respuesta = gRPCrespuesta(respuesta, True)
    return respuesta
  elif 'buscarArchivo' in val:
    ip = val.split('%')[1]
    nombreArchivo = val[val.index('&')+1:val.index('%')]
    listar = buscarArchivo(nombreArchivo)
    respuesta = f'{str(listar)}&{ip}'
    respuesta = encriptar(respuesta)
    respuesta = gRPCrespuesta(respuesta, True)
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

def gRPCrespuesta(request, tipoDeRetorno):
  channel = grpc.insecure_channel(f'127.0.0.1:8080')
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.message(messages_pb2.instructionRequest(query=request, respuesta=tipoDeRetorno))
  response  = MessageToDict(response)
  return response 

def gRPC(request):
  channel = grpc.insecure_channel(f'127.0.0.1:8080')
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.message(messages_pb2.instructionRequest(query=request))
  response  = MessageToDict(response)
  return response 

def encriptar(texto):
  texto = texto.encode('utf-8') 
  texto = base64.b64encode(texto).decode('utf-8')
  return texto

if __name__ == '__main__':
  uvicorn.run(app, host="127.0.0.1", port=8002)
  #conexionCola('cola1')
  #suscribirse('topico1', 'Sara')
  #conexionTopico('topico1', 'Sara')
