class Cola:

  def __init__(self):
    self.cola = []

  def agregar(self, mensaje):
    self.cola.append(mensaje)

  def consumir(self):
    if self.cola:
      mensaje = self.cola.pop(0)
    else:
      mensaje = 'cola vacia'
    return mensaje
  
  def mirar(self):
    return self.cola[0]