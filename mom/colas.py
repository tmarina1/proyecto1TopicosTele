class Queue:
  def __init__(self):
    self.items = []

  def enqueue(self, item):
    self.items.append(item)

  def dequeue(self):
    return self.items.pop(0)

  def is_empty(self):
    return len(self.items) == 0

colas = {}

def crearCola(nombreCola):
  colas[f'{nombreCola}'] = Queue()

def agregarElemento(nombreCola, info):
  colas[f'{nombreCola}'].enqueue(info)

def eliminarElemento(nombreCola):
  colas[f'{nombreCola}'].dequeue()

def listarElementosCola(nombreCola):
  lista = []
  copiaCola = Queue()
  for elemento in colas[f'{nombreCola}'].items:
    copiaCola.enqueue(elemento)
    lista.append(elemento)
  return lista

def verElemento(nombreCola):
  item = colas[f'{nombreCola}'].dequeue()
  return item

def mostrarColas():
  return colas.keys()
