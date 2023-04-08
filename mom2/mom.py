import grpc
import messages_pb2
import messages_pb2_grpc
from concurrent import futures
from cola import Cola
from topicos import Topic
from topicos import *
from google.protobuf.json_format import MessageToDict
import pickle
import re

class messageService(messages_pb2_grpc.messageServiceServicer):
  def __init__(self) -> None:
    super().__init__()
    self.colas = {}
    self.colasRespuestas = {}
    self.topicos = {}

  def sync(self, request, context):
    if request:
      print('mandoDatosActualizadosMom2')
      request = request.estado
      respuesta = pickle.loads(request)
      self.colas = respuesta[0]
      self.colasRespuestas = respuesta[1]
      self.topicos = respuesta[2]
      return messages_pb2.messageResponse(results=f"Sincronización recibida")
    else:
      return messages_pb2.messageResponse(results=f"Sincronización no recibida")
  def message(self, request, context):
    print('En mom2')
    print(request.query)
    print(f'Hola: {request}')
    if request:
      query = request.query
      respuestaMS = request.respuesta
      print(respuestaMS)
      request = str(request)
      if "crearCola" in query:
        nombreCola = request.replace('\n', '').replace('\\', '')
        nombreCola = nombreCola.split('/')[-1].strip('"n')
        self.colas[nombreCola] = Cola()
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "agregarElementoCola" in query:
        nombreCola = request.split('/')[1]
        mensaje = request.split('/')[2][:-2]
        self.colas[nombreCola].agregar(mensaje)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "listarColas" in query:
        todasLasColas = self.colas.keys()
        return messages_pb2.messageResponse(results=f"Respuesta del servicio: {todasLasColas}")
      elif "borrarCola" in query:
        nombreCola = request.replace('\n', '').replace('\\', '')
        nombreCola = nombreCola.split('/')[-1].strip('"n')
        if nombreCola in self.colas:
          del self.colas[nombreCola]
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado)
          return messages_pb2.messageResponse(results=f"Cola eliminada")
        else:
          return messages_pb2.messageResponse(results=f"Cola no existe")
      elif respuestaMS: #Respuestas del microservicio
        mensaje = request[:request.index('&')]
        cliente = re.search(r'\d+\.\d+\.\d+\.\d+', request).group()
        if cliente in self.colasRespuestas:
          self.colasRespuestas[cliente].agregar(mensaje)
        else:
          self.colasRespuestas[cliente] = Cola()
          self.colasRespuestas[cliente].agregar(mensaje)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "consumir" in query:
        cliente = re.search(r'\d+\.\d+\.\d+\.\d+', request).group()
        consulta = self.colasRespuestas[cliente].consumir()
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
        return messages_pb2.messageResponse(results=f"Respuesta del servicio {consulta}")
      elif "crearTopico" in query:
        nombreTopico = request.replace('\n', '').replace('\\', '')
        nombreTopico = nombreTopico.split('/')[-1].strip('"n')
        self.topicos[nombreTopico] = Topic()
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "agregarMensajeTopico" in query:
        nombreTopico = request.split('/')[1]
        mensaje = request.split('/')[2][:-2]
        self.topicos[nombreTopico].publicar(mensaje)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "verMensajesTopico" in query:
        nombreTopico = request.split('/', 1)[1][:-2]
        verTopico = self.topicos[nombreTopico]
        return messages_pb2.messageResponse(results=f"{str(verTopico)}")
      elif "suscribirTopico" in query:
        nombreTopico = request.split('/')[1]
        nombreSuscriptor = request.split('/')[2][:-2]
        self.topicos[nombreTopico].suscribir(nombreSuscriptor)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "eliminarTopico" in query:
        nombreTopico = request.replace('\n', '').replace('\\', '')
        nombreTopico = nombreTopico.split('/')[-1].strip('"n')
        if nombreTopico in self.topicos:
          del self.topicos[nombreTopico]
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado)
          return messages_pb2.messageResponse(results=f"Topico eliminado")
        else:
          return messages_pb2.messageResponse(results=f"Topico no existe")
      elif "cCola" in query:
        print('holii')
        nombreCola = request.split('/')[1][:-2]
        respuesta = self.colas[nombreCola].consumir()
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
        return messages_pb2.messageResponse(results=f"{str(respuesta)}")
      elif "conTopico" in query:
        nombreTopico = request.split('/')[1]
        nombreSuscriptor = request.split('/')[2][:-2]
        respuesta = self.topicos[nombreTopico].consumir(nombreSuscriptor)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
        return messages_pb2.messageResponse(results=f"{str(respuesta)}")
      return messages_pb2.messageResponse(results=f"Petición recibida")
    else:
      return messages_pb2.messageResponse(results=f"Petición no recibida")

def gRPCreplicacion(request):
  request = pickle.dumps(request)
  channel = grpc.insecure_channel(f'127.0.0.1:8080')
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.sync(messages_pb2.instructionRequest(estado=request))
  return response 

def serve():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  messages_pb2_grpc.add_messageServiceServicer_to_server(messageService(), server)
  server.add_insecure_port('[::]:8081')
  server.start()
  server.wait_for_termination()

if __name__ == '__main__':
  serve()