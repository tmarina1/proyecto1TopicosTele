import grpc
import messages_pb2
import messages_pb2_grpc
import os
from concurrent import futures
from colas import crearCola, agregarElemento, mostrarColas, listarElementosCola, verElemento
from topicos import Topico
from topicos import *

queue = 0
Topic = Topico()

class messageService(messages_pb2_grpc.messageServiceServicer):
  def message(self, request, context):
    request = str(request)
    print(f'Hola: {request}')
    
    if request:
      if "crearCola" in request:
        nombreCola = request.split('/', 1)[1][:-2]
        global queue
        queue = crearCola(f'{nombreCola}')
      elif "agregarElemento" in request:
        nombreCola = request.split('/')[1]
        mensaje = request.split('/')[2][:-2]
        agregarElemento(nombreCola, mensaje)
      elif "listarColas" in request:
        print(mostrarColas())
      elif "listarElementosCola" in request:
        nombreCola = request.split('/', 1)[1][:-2]
        print(listarElementosCola(nombreCola))
      elif "verCola" in request:
        nombreCola = request.split('/', 1)[1][:-2]
        print(conexion(nombreCola))
      elif "2" in request:
        nombreCola = 'respuestas'
        crearCola(f'{nombreCola}')
        mensaje = request[request.index("query:") + len("query:"):].strip()
        agregarElemento(nombreCola, mensaje)
      elif "verRespuesta" in request:
        nombreCola = 'respuestas'
        consulta = verElemento(nombreCola)
        print(consulta)
        return messages_pb2.messageResponse(results=f"Respuesta del servicio {consulta}")
      if "crearTopico" in request:
        nombreTopico = request.split('/', 1)[1][:-2]
        global Topic
        Topic.agregarTopico(f'{nombreTopico}')
        #print(Topic.obtenerTopicos())
      if "agregarMensajeTopico" in request:
        nombreTopico = request.split('/')[1]
        mensaje = request.split('/')[2][:-2]
        Topic.publicar(nombreTopico, mensaje)
      if "verMensajesTopico" in request:
        nombreTopico = request.split('/', 1)[1][:-2]
        print(Topic.verTodosLosMensajes(f'{nombreTopico}'))

      return messages_pb2.messageResponse(results=f"Petición recibida")
    else:
      return messages_pb2.messageResponse(results=f"Petición no recibida")

def getTopic():
  return Topic.obtenerTopicos()

def conexion(nombreCola):
  peticion = verElemento(nombreCola)
  if peticion == 'listarArchivos':
    return listarArchivos()
  elif peticion == 'buscarArchivo':
    pass  

def listarArchivos():
  listaDirectorios = os.listdir('/')
  return listaDirectorios

def serve():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  messages_pb2_grpc.add_messageServiceServicer_to_server(messageService(), server)
  server.add_insecure_port('[::]:8080')
  server.start()
  server.wait_for_termination()

if __name__ == '__main__':
  serve()