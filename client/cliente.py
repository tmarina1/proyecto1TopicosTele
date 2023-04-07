import os
import sys
import grpc
import messages_pb2
import messages_pb2_grpc
from google.protobuf.json_format import MessageToDict

def conexionCola(nombreCola):
  peticion = gRPC(f'cCola/{nombreCola}')
  val = ''.join(peticion['results'])
  ip = val.split('%')[1]
  if 'listarArchivos' in val:
    listar = listarArchivos()
    respuesta = gRPCrespuesta(f'{str(listar)}&{ip}', True)
    return respuesta
  elif 'buscarArchivo' in val:
    nombreArchivo = peticion.split('&')[1]
    listar = buscarArchivo(nombreArchivo)
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
    nombreArchivo = peticion.split('&')[1]
    listar = buscarArchivo(nombreArchivo)
    respuesta = gRPCrespuesta(f'{str(listar)}&{ip}', True)
    return respuesta

def listarArchivos():
  listaDirectorios = os.listdir('/')
  return listaDirectorios

def buscarArchivo(nombreArchivo):
  isFound = 'File not found.'
  p = join('/',nombreArchivo)
  for root, dirs, files in walk(p):
      if nombreArchivo in files:
          isFound = 'Exists!'
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
  #conexionCola('cola1')
  #suscribirse('topico1', 'Sara')
  conexionTopico('topico1', 'Sara')
