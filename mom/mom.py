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
      respuesta = pickle.loads(request)
      self.colas = respuesta['query'][0]
      self.colasRespuestas = respuesta['query'][1]
      self.topicos = respuesta['query'][2]
      return messages_pb2.messageResponse(results=f"Sincronización recibida")
    else:
      return messages_pb2.messageResponse(results=f"Sincronización no recibida")
  def message(self, request, context):
    request = str(request)
    print(f'Hola: {request}')
    if request:
      if "1111" in request:
        if "crearCola" in request:
          nombreCola = request.replace('\n', '').replace('\\', '')
          nombreCola = nombreCola.split('/')[-1].strip('"n')
          crearCola(f'{nombreCola}')
        elif "agregarElemento" in request:
          nombreCola = request.split('/')[1]
          mensaje = request.replace('\n', '').replace('\\', '')
          mensaje = mensaje.split('/')[-1].strip('"n')
          agregarElemento(nombreCola, mensaje)
        elif "listarColas" in request:
          todasLasColas = mostrarColas()
        elif "listarElementosCola" in request:
          nombreCola = request.replace('\n', '').replace('\\', '')
          nombreCola = nombreCola.split('/')[-1].strip('"n')
          elementosCola = listarElementosCola(nombreCola)
          print(elementosCola)
        elif "2354" in request: #Respuestas del microservicio
          mensaje = request[request.index("query:") + len("query:"):].strip()
          cliente = ''
          ColaRespuesta.agregar(cliente, mensaje)
        elif "verRespuesta" in request:
          cliente = ''
          consulta = ColaRespuesta.consumir(cliente)
          return messages_pb2.messageResponse(results=f"Respuesta del servicio {consulta}")
        elif "crearTopico" in request:
          nombreTopico = request.replace('\n', '').replace('\\', '')
          nombreTopico = nombreTopico.split('/')[-1].strip('"n')
          print(nombreTopico)
          Topico.crearTopico(nombreTopico)
        elif "agregarMensajeTopico" in request:
          nombreTopico = request.split('/')[1]
          mensaje = request.split('/')[2][:-2]
          Topico.publicar(nombreTopico, mensaje)
        elif "verMensajesTopico" in request:
          nombreTopico = request.split('/', 1)[1][:-2]
          print(Topico.verTodosLosMensajes(f'{nombreTopico}'))
        elif "suscribirseTopico" in request:
          nombreTopico = request.split('/')[1]
          nombreSuscriptor = request.replace('\n', '').replace('\\', '')
          nombreSuscriptor = nombreSuscriptor.split('/')[-1].strip('"n')
          Topico.suscribir(nombreSuscriptor, nombreTopico)
        elif "verElementoMS" in request:
          nombreCola = request.split('/')[1][:-2]
          respuesta = verElemento(nombreCola)
          return messages_pb2.messageResponse(results=f"{str(respuesta)}")
        elif "verDatosEnTopico" in request:
          nombreTopico = request.split('/')[1]
          nombreSuscriptor = request.split('/')[2][:-2]
          respuesta = Topico.consumir(nombreSuscriptor, nombreTopico)
          return messages_pb2.messageResponse(results=f"{str(respuesta)}")
      else:
        if "crearCola" in request:
          nombreCola = request.replace('\n', '').replace('\\', '')
          nombreCola = nombreCola.split('/')[-1].strip('"n')
          crearCola(f'{nombreCola}')
          estado = [self.colas, self.colasRespuestas, self.topicos]
          gRPCreplicacion(estado, 2222)
        elif "agregarElemento" in request:
          nombreCola = request.split('/')[1]
          mensaje = request.split('/')[2][:-2]
          agregarElemento(nombreCola, mensaje)
          gRPCreplicacion(request, 2222)
        elif "listarColas" in request:
          todasLasColas = mostrarColas()
          gRPCreplicacion(request, 2222)
          return messages_pb2.messageResponse(results=f"Respuesta del servicio: {todasLasColas}")
        elif "listarElementosCola" in request:
          nombreCola = request.split('/', 1)[1][:-2]
          elementosCola = listarElementosCola(nombreCola)
          gRPCreplicacion(request, 2222)
          return messages_pb2.messageResponse(results=f"Respuesta del servicio: {elementosCola}")
        elif "2354" in request: #Respuestas del microservicio
          mensaje = request[request.index("query:") + len("query:"):].strip()
          cliente = mensaje.split('&')[1].replace('"', '')
          print(cliente)
          ColaRespuesta.agregar(cliente, mensaje)
          gRPCreplicacion(request, 2222)
        elif "verRespuesta" in request:
          #cliente = str(request.split('&')[1].replace('"', '').replace(" ", ""))
          cliente = '127.0.0.1'
          print(cliente)
          consulta = ColaRespuesta.consumir(cliente)
          gRPCreplicacion(request, 2222)
          return messages_pb2.messageResponse(results=f"Respuesta del servicio {consulta}")
        elif "crearTopico" in request:
          nombreTopico = request.replace('\n', '').replace('\\', '')
          nombreTopico = nombreTopico.split('/')[-1].strip('"n')
          Topico.crearTopico(nombreTopico)
          gRPCreplicacion(request, 2222)
        elif "agregarMensajeTopico" in request:
          nombreTopico = request.split('/')[1]
          mensaje = request.split('/')[2][:-2]
          Topico.publicar(mensaje, nombreTopico)
          gRPCreplicacion(request, 2222)
        elif "verMensajesTopico" in request:
          nombreTopico = request.split('/', 1)[1][:-2]
          verTopico = Topico.verTopicos()
          print(verTopico)
          gRPCreplicacion(request, 2222)
          return messages_pb2.messageResponse(results=f"{str(verTopico)}")
        elif "suscribirseTopico" in request:
          nombreTopico = request.split('/')[1]
          nombreSuscriptor = request.split('/')[2][:-2]
          Topico.suscribir(nombreSuscriptor, nombreTopico)
          gRPCreplicacion(request, 2222)
        elif "verElementoMS" in request:
          nombreCola = request.split('/')[1][:-2]
          respuesta = verElemento(nombreCola)
          gRPCreplicacion(request, 2222)
          return messages_pb2.messageResponse(results=f"{str(respuesta)}")
        elif "verDatosEnTopico" in request:
          nombreTopico = request.split('/')[1]
          nombreSuscriptor = request.split('/')[2][:-2]
          respuesta = Topico.consumir(nombreSuscriptor, nombreTopico)
          gRPCreplicacion(request, 2222)
          return messages_pb2.messageResponse(results=f"{str(respuesta)}")

      return messages_pb2.messageResponse(results=f"Petición recibida")
    else:
      return messages_pb2.messageResponse(results=f"Petición no recibida")

def gRPCreplicacion(request, tipoDeRetorno):
  request = pickle.dumps(request)
  channel = grpc.insecure_channel(f'127.0.0.1:8081')
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.message(messages_pb2.instructionRequest(query=request, limit=tipoDeRetorno))
  response  = MessageToDict(response)
  return response 

def serve():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  messages_pb2_grpc.add_messageServiceServicer_to_server(messageService(), server)
  server.add_insecure_port('[::]:8080')
  server.start()
  server.wait_for_termination()

if __name__ == '__main__':
  serve()