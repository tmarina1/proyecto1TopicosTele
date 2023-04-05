import queue
class Topico:
  def __init__(self):
    self.topicos = {}
    self.suscriptores = []
    self.visto = {}

  def agregarTopico(self, topico):
    if topico not in self.topicos:
      self.topicos[topico] = queue.Queue()

  def agregarSuscriptor(self, suscriptor):
    self.suscriptores.append(suscriptor)
    self.visto[suscriptor] = 0

  def publicar(self, topico, mensaje):
    if topico in self.topicos:
      self.topicos[topico].put(mensaje)
    else:
      pass

  def verMensaje(self, topico, suscriptor):
    if topico in self.topicos and not self.topicos[topico].empty():
      if suscriptor in self.visto:
        self.visto[suscriptor] += 1
        if all(vistos >= 1 for vistos in self.visto.values()):
          return self.topicos[topico].get()
        else:
          return self.topicos[topico].queue[0]

  def verTodosLosMensajes(self, topico):
    mensajes = []
    if topico in self.topicos:
      while not self.topicos[topico].empty():
        mensajes.append(self.topicos[topico].get())
      for mensaje in mensajes:
        self.topicos[topico].put(mensaje)
    return mensajes
  
  def obtenerTopicos(self):
    return self.topicos
  
#Yo propongo que quede asi
#El mom tendria un diccionario global -> nombreTopico: Topic()
from queue import Queue
class Topic:

  def __init__(self, nombre):
    self.suscriptores = {}

  def suscribir(self, suscriptor):
    self.suscriptores[suscriptor] = Queue(maxsize=0)

  def desuscribir(self, suscriptor):
    if self.suscriptores.get(suscriptor) is not None:
      del self.suscriptores[suscriptor]
  
  def publicar(self, mensaje):
    for suscriptor in self.suscriptores.keys():
      self.suscriptores[suscriptor].put(mensaje)

  def consumir(self, suscriptor):
    cola = self.suscriptores.get(suscriptor)
    if cola is not None:
      try:
        mensaje = cola.get_nowait()
      except:
        mensaje = 'cola vacia'

    return mensaje