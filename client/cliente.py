import os
import sys
sys.path.append('../mom')
from colas import colas as cola
from colas import Queue
import colas

def conexion(nombreCola):
  print(cola)
  peticion = colas.verElemento(nombreCola)
  if peticion == 'listarArchivos':
    print(listarArchivos())
  elif peticion == 'buscarArchivo':
    pass  

def listarArchivos():
  listaDirectorios = os.listdir('/mom')
  return listaDirectorios

if __name__ == '__main__':
  conexion('cola1')