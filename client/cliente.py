import os
import grpc
import messages_pb2
import messages_pb2_grpc
import uvicorn
from fastapi import FastAPI, responses, Request
from google.protobuf.json_format import MessageToDict

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
  peticion = gRPC(f'cCola/{nombreCola}')
  val = ''.join(peticion['results'])
  ip = val.split('%')[1]
  if 'listarArchivos' in val:
    listar = listarArchivos()
    respuesta = gRPCrespuesta(f'{str(listar)}&{ip}', True)
    return respuesta
  elif 'buscarArchivo' in val:
    nombreArchivo = val[val.index('&')+1:val.index('%')]
    listar = buscarArchivo(nombreArchivo)
    print(listar)
    respuesta = gRPCrespuesta(f'{str(listar)}&{ip}', True)
    return respuesta

def suscribirse(nombreTopico, nombreSuscriptor):
  gRPC(f'suscribirTopico/{nombreTopico}/{nombreSuscriptor}')

def conexionTopico(nombreTopico, nombreSuscriptor):
  peticion = gRPC(f'conTopico/{nombreTopico}/{nombreSuscriptor}')
  val = ''.join(peticion['results'])
  ip = val.split('%')[1]
  if 'listarArchivos' in val:
    listar = listarArchivos()
    respuesta = gRPCrespuesta(f'{str(listar)}&{ip}', True)
    return respuesta
  elif 'buscarArchivo' in val:
    nombreArchivo = val[val.index('&')+1:val.index('%')]
    listar = buscarArchivo(nombreArchivo)
    respuesta = gRPCrespuesta(f'{str(listar)}&{ip}', True)
    return respuesta

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

if __name__ == '__main__':
  uvicorn.run(app, host="127.0.0.1", port=8002)
  #conexionCola('cola1')
  #suscribirse('topico1', 'Sara')
  #conexionTopico('topico1', 'Sara')
