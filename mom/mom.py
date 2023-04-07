import grpc
import messages_pb2
import messages_pb2_grpc
from concurrent import futures
from colas import crearCola, agregarElemento, mostrarColas, listarElementosCola, verElemento
from colaRespuesta import colaRespuestas
from topicos import Topic
from topicos import *
from google.protobuf.json_format import MessageToDict
import pickle

Topico = Topic()
ColaRespuesta = colaRespuestas()

class messageService(messages_pb2_grpc.messageServiceServicer):
  def __init__(self) -> None:
    super().__init__()
    self.colas = {}
    self.colasRespuestas = {}
    self.topicos = {}

  def sync(self, request, context):
    if request:
      request = request.estado
      respuesta = pickle.loads(request)
      self.colas = respuesta[0]
      self.colasRespuestas = respuesta[1]
      self.topicos = respuesta[2]
      return messages_pb2.messageResponse(results=f"Sincronización recibida")
    else:
      return messages_pb2.messageResponse(results=f"Sincronización no recibida")

  def message(self, request, context):
    request = str(request)
    print(f'Hola: {request}')
    if request:
      if "crearCola" in request:
        nombreCola = request.replace('\n', '').replace('\\', '')
        nombreCola = nombreCola.split('/')[-1].strip('"n')
        self.colas[nombreCola] = Queue()
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "agregarElemento" in request:
        nombreCola = request.split('/')[1]
        mensaje = request.split('/')[2][:-2]
        self.colas[nombreCola].agregarMensaje(mensaje)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "listarColas" in request:
        todasLasColas = self.colas
        return messages_pb2.messageResponse(results=f"Respuesta del servicio: {todasLasColas}")
      elif "listarElementosCola" in request:
        nombreCola = request.split('/', 1)[1][:-2]
        elementosCola = self.colas[nombreCola]
        return messages_pb2.messageResponse(results=f"Respuesta del servicio: {elementosCola}")
      elif "2354" in request: #Respuestas del microservicio
        mensaje = request[request.index("query:") + len("query:"):].strip()
        cliente = mensaje.split('&')[1].replace('"', '')
        print(cliente)
        ColaRespuesta.agregar(cliente, mensaje)
        gRPCreplicacion(request, 2222)
      elif "verRespuesta" in request:
        cliente = str(request.split('&')[1].replace('"', '').replace(" ", ""))
        cliente = '127.0.0.1'
        consulta = ColaRespuesta.consumir(cliente)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
        return messages_pb2.messageResponse(results=f"Respuesta del servicio {consulta}")
      elif "crearTopico" in request:
        nombreTopico = request.replace('\n', '').replace('\\', '')
        nombreTopico = nombreTopico.split('/')[-1].strip('"n')
        self.topicos[nombreTopico] = Topic()
        self.topicos[nombreTopico].suscribir('Tomas')
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "agregarMensajeTopico" in request:
        nombreTopico = request.split('/')[1]
        mensaje = request.split('/')[2][:-2]
        self.topicos[nombreTopico].publicar(mensaje)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        print(self.topicos['topico1'].suscriptores['Tomas'])
        gRPCreplicacion(estado)
      elif "verMensajesTopico" in request:
        nombreTopico = request.split('/', 1)[1][:-2]
        verTopico = self.topicos[nombreTopico]
        return messages_pb2.messageResponse(results=f"{str(verTopico)}")
      elif "suscribirseTopico" in request:
        nombreTopico = request.split('/')[1]
        nombreSuscriptor = request.split('/')[2][:-2]
        self.topicos[nombreTopico].suscribir(nombreSuscriptor)
        estado = [self.colas, self.colasRespuestas, self.topicos]
        gRPCreplicacion(estado)
      elif "verElementoMS" in request:
        nombreCola = request.split('/')[1][:-2]
        respuesta = verElemento(nombreCola)
        gRPCreplicacion(request)
        return messages_pb2.messageResponse(results=f"{str(respuesta)}")
      elif "verDatosEnTopico" in request:
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