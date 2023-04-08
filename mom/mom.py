import grpc
import messages_pb2
import messages_pb2_grpc
from concurrent import futures
from cola import Cola
from topicos import Topic
from google.protobuf.json_format import MessageToDict
import pickle
import re
import base64

class messageService(messages_pb2_grpc.messageServiceServicer):
  def __init__(self) -> None:
    super().__init__()
    self.colas = {}
    self.colasRespuestas = {}
    self.topicos = {}

  def sync(self, request, context):
    if request:
      print('mandoDatosActualizadosMom1')
      request = request.estado
      respuesta = pickle.loads(request)
      self.colas = respuesta[0]
      self.colasRespuestas = respuesta[1]
      self.topicos = respuesta[2]
      return messages_pb2.messageResponse(results=f"Sincronización recibida")
    else:
      return messages_pb2.messageResponse(results=f"Sincronización no recibida")

  def message(self, request, context):
    print('En mom1')
    print(f'Hola: {request}')
    if request:
      query = desencriptar(request.query)
      #query = request.query
      print(query)
      respuestaMS = request.respuesta
      request = str(request)
      if "crearCola" in query:
        nombreCola = query.replace('\n', '').replace('\\', '')
        nombreCola = nombreCola.split('/')[-1].strip('"n')
        self.colas[nombreCola] = Cola()
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "agregarElementoCola" in query:
        nombreCola = query.split('/')[1]
        mensaje = query.split('/')[2]
        try:
          self.colas[nombreCola].agregar(mensaje)
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado)
        except:
          return messages_pb2.messageResponse(results=f"Porfavor vuelva a mandar la petición")
      elif "listarColas" in query:
        todasLasColas = self.colas.keys()
        return messages_pb2.messageResponse(results=f"Respuesta del servicio: {todasLasColas}")
      elif "borrarCola" in query:
        nombreCola = query.replace('\n', '').replace('\\', '')
        nombreCola = nombreCola.split('/')[-1].strip('"n')
        if nombreCola in self.colas:
          del self.colas[nombreCola]
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado)
          return messages_pb2.messageResponse(results=f"Cola eliminada")
        else:
          return messages_pb2.messageResponse(results=f"Cola no existe")
      elif respuestaMS: #Respuestas del microservicio
        mensaje = query[:query.index('&')]
        cliente = re.search(r'\d+\.\d+\.\d+\.\d+', query).group()
        if cliente in self.colasRespuestas:
          self.colasRespuestas[cliente].agregar(mensaje)
        else:
          self.colasRespuestas[cliente] = Cola()
          self.colasRespuestas[cliente].agregar(mensaje)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "consumir" in query:
        cliente = re.search(r'\d+\.\d+\.\d+\.\d+', query).group()
        consulta = self.colasRespuestas[cliente].consumir()
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
        return messages_pb2.messageResponse(results=f"Respuesta del servicio {consulta}")
      elif "crearTopico" in query:
        nombreTopico = query.replace('\n', '').replace('\\', '')
        nombreTopico = nombreTopico.split('/')[-1].strip('"n')
        self.topicos[nombreTopico] = Topic()
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "agregarMensajeTopico" in query:
        nombreTopico = query.split('/')[1]
        mensaje = query.split('/')[2]
        try:
          self.topicos[nombreTopico].publicar(mensaje)
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado)
        except:
          return messages_pb2.messageResponse(results="El topico al que decea agregar el mensaje no existe")
      elif "verMensajesTopico" in query:
        nombreTopico = query.split('/', 1)[1]
        try:
          verTopico = self.topicos[nombreTopico]
          return messages_pb2.messageResponse(results=f"{str(verTopico)}")
        except:
          return messages_pb2.messageResponse(results="No existe el topico ingresado")
      elif "suscribirTopico" in query:
        nombreTopico = query.split('/')[1]
        nombreSuscriptor = query.split('/')[2]
        try:
          self.topicos[nombreTopico].suscribir(nombreSuscriptor)
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado)
          return messages_pb2.messageResponse(results="Se suscribio correctamente")
        except:
          return messages_pb2.messageResponse(results="Topico no existe")
      elif "eliminarTopico" in query:
        nombreTopico = query.replace('\n', '').replace('\\', '')
        nombreTopico = nombreTopico.split('/')[-1].strip('"n')
        if nombreTopico in self.topicos:
          del self.topicos[nombreTopico]
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado)
          return messages_pb2.messageResponse(results=f"Topico eliminado")
        else:
          return messages_pb2.messageResponse(results=f"Topico no existe")
      elif "cCola" in query:
        nombreCola = query.split('/')[1]
        try:
          respuesta = self.colas[nombreCola].consumir()
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado)
          return messages_pb2.messageResponse(results=f"{str(respuesta)}")
        except:
          return messages_pb2.messageResponse(results="Cola a consumir no existe")
      elif "conTopico" in query:
        nombreTopico = query.split('/')[1]
        nombreSuscriptor = query.split('/')[2]
        try:
          respuesta = self.topicos[nombreTopico].consumir(nombreSuscriptor)
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado)
          return messages_pb2.messageResponse(results=f"{str(respuesta)}")
        except:
          return messages_pb2.messageResponse(results="Suscriptor o topico a consumir no existen")
      return messages_pb2.messageResponse(results=f"Petición recibida")
    else:
      return messages_pb2.messageResponse(results=f"Petición no recibida")

def desencriptar(texto):
  texto = base64.b64decode(texto)
  texto = texto.decode("utf-8")
  return texto

def gRPCreplicacion(request):
  request = pickle.dumps(request)
  channel = grpc.insecure_channel(f'127.0.0.1:8081')
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.sync(messages_pb2.instructionRequest(estado=request))
  return response 

def serve():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  messages_pb2_grpc.add_messageServiceServicer_to_server(messageService(), server)
  server.add_insecure_port('[::]:8080')
  server.start()
  server.wait_for_termination()

if __name__ == '__main__':
  serve()