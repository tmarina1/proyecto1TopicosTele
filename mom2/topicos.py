from queue import Queue

class Topic:
  def __init__(self):
    self.topicos = {}

  def crearTopico(self, nombre):
    if nombre not in self.topicos:
      self.topicos[nombre] = {}

  def suscribir(self, suscriptor, nombre_topico):
    if nombre_topico in self.topicos:
      self.topicos[nombre_topico][suscriptor] = Queue(maxsize=0)

  def desuscribir(self, suscriptor, nombre_topico):
    if nombre_topico in self.topicos:
      if self.topicos[nombre_topico].get(suscriptor) is not None:
        del self.topicos[nombre_topico][suscriptor]

  def publicar(self, mensaje, nombre_topico):
    if nombre_topico in self.topicos:
      for suscriptor in self.topicos[nombre_topico].keys():
        print(suscriptor)
        self.topicos[nombre_topico][suscriptor].put(mensaje)
    else:
      print("no dio")

  def consumir(self, suscriptor, nombre_topico):
    if nombre_topico in self.topicos:
      if self.topicos[nombre_topico].get(suscriptor) is not None:
        cola = self.topicos[nombre_topico].get(suscriptor)
        if cola is not None:
          try:
            mensaje = cola.get_nowait()
          except:
            mensaje = 'cola vacia'
        return mensaje
        
  def verTopicos(self):
    return self.topicos