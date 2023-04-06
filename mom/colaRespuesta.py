from queue import Queue
class colaRespuestas:

  def __init__(self):
    self.respuestas = {}

  def agregar(self, cliente, mensaje):
    cola = self.respuestas.get(cliente)
    if cola is not None:
      cola.put(mensaje)
    else:
      self.respuestas[cliente] = Queue(maxsize=0)
      self.respuestas[cliente].put(mensaje)

  def consumir(self, cliente):
    cola = self.respuestas.get(cliente)
    mensaje = ''
    if cola:
      try:
        mensaje = cola.get_nowait()
      except:
        mensaje = 'cola vacia'
    return mensaje