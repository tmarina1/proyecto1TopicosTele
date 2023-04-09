#Yo propongo que quede asi
#El mom tendria un diccionario global -> nombreTopico: Topic()
from queue import Queue
class Topic:

  def __init__(self):
    self.suscriptores = {}

  def suscribir(self, suscriptor):
    self.suscriptores[suscriptor] = []

  def desuscribir(self, suscriptor):
    if self.suscriptores.get(suscriptor) is not None:
      del self.suscriptores[suscriptor]

  def publicar(self, mensaje):
    for suscriptor in self.suscriptores.keys():
      self.suscriptores[suscriptor].append(mensaje)
      print(suscriptor)

  def consumir(self, suscriptor):
    cola = self.suscriptores.get(suscriptor)
    mensaje = ''
    if cola is not None:
      try:
        mensaje = cola.get_nowait()
      except:
        mensaje = 'cola vacia'
    else:
      mensaje = 'Usuario no esta suscrito'
    return mensaje