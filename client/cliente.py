import os
import sys
import grpc
import messages_pb2
import messages_pb2_grpc
sys.path.append('../mom')
from google.protobuf.json_format import MessageToDict

def conexionCola(nombreCola):
  peticion = gRPC(f'verElementoMS/{nombreCola}')
  val = ''.join(peticion['results'])
  ip = val.split('%')[1]
  if 'listarArchivos' in val:
    listar = listarArchivos()
    respuesta = gRPCrespuesta(f'{str(listar)}&{ip}', 2354)
    return respuesta
  elif 'buscarArchivo' in val:
    nombreArchivo = peticion.split('&')[1]
    listar = buscarArchivo(nombreArchivo)
    respuesta = gRPC(str(listar))
    return respuesta

def suscribirse(nombreTopico, nombreSuscriptor):
  gRPC(f'suscribirseTopico/{nombreTopico}/{nombreSuscriptor}')

def conexionTopico(nombreTopico, nombreSuscriptor):
  peticion = gRPC(f'verDatosEnTopico/{nombreTopico}/{nombreSuscriptor}')
  val = ''.join(peticion['results'])
  ip = val.split('%')[1]
  if 'listarArchivos' in val:
    listar = listarArchivos()
    respuesta = gRPCrespuesta(f'{str(listar)}&{ip}', 2354)
    return respuesta
  elif 'buscarArchivo' in val:
    nombreArchivo = peticion.split('&')[1]
    listar = buscarArchivo(nombreArchivo)
    respuesta = gRPC(f'{str(listar)}&{ip}', 2354)
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

if __name__ == '__main__':
  #conexionCola('cola1')
  #suscribirse('topico1', 'Pedro')
  conexionTopico('topico1', 'Pedro')
