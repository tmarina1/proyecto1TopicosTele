import os
import sys
import grpc
import messages_pb2
import messages_pb2_grpc
sys.path.append('../mom')
from mom import queue as colas
from mom import Topic
from mom import getTopic
from google.protobuf.json_format import MessageToDict

def conexionCola(nombreCola):
  peticion = gRPC(f'verElementoMS/{nombreCola}')
  val = ''.join(peticion['results'])
  if 'listarArchivos' in val:
    listar = listarArchivos()
    respuesta = gRPCrespuesta(str(listar), 2)
    return respuesta
  elif 'buscarArchivo' in val:
    nombreArchivo = peticion.split('&')[1]
    listar = buscarArchivo(nombreArchivo)
    respuesta = gRPC(str(listar))
    return respuesta

def conexionTopico(nombreTopico, nombreSuscriptor):
  peticion = gRPC(f'verElementoMS/{nombreTopico}')
  val = ''.join(peticion['results'])
  if 'listarArchivos' in val:
    listar = listarArchivos()
    respuesta = gRPC(str(listar), 2)
    return respuesta
  elif 'buscarArchivo' in val:
    nombreArchivo = peticion.split('&')[1]
    listar = buscarArchivo(nombreArchivo)
    respuesta = gRPC(str(listar), 2)
    return respuesta

def conexionPruebas():
  peticion = 'listarArchivos'
  if peticion == 'listarArchivos':
    print(listarArchivos())
    listar = listarArchivos()
    respuesta = gRPC(str(listar), 2)
    print(respuesta)
  elif peticion == 'buscarArchivo':
    pass 

def listarArchivos():
  listaDirectorios = os.listdir('/')
  return listaDirectorios

def buscarArchivo(nombreArchivo):
  archivo = []
  for root, dirs, files in os.walk('/'):
    for file in files:
      if nombreArchivo in file:
        archivo.append(os.path.join(root, file))
  return archivo

def gRPCrespuesta(request, tipoDeRetorno):
  channel = grpc.insecure_channel(f'127.0.0.1:8080')
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.message(messages_pb2.instructionRequest(query=request, limit=tipoDeRetorno))
  response  = MessageToDict(response)
  return response 

def gRPC(request):
  channel = grpc.insecure_channel(f'127.0.0.1:8080')
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.message(messages_pb2.instructionRequest(query=request))
  response  = MessageToDict(response)
  return response 

def prueba():
  print(Topic.obtenerTopicos())

if __name__ == '__main__':
  #conexionPruebas()
  conexionCola('cola1')
  conexionTopico('topico1', 'MS1')
  #prueba()
  #print(getTopic())