import queue

topicos = {}

def agregarTopico(nombreTopico):
    topicos[nombreTopico] = queue.Queue()

def eliminarTipico(nombreTopico):
    topicos.pop(nombreTopico, None)

def publicar(topico, mensaje):
    mensajesEnCola = topicos.get(topico)
    if mensajesEnCola:
        mensajesEnCola.put(mensaje)

def verMensaje(nombreTopico):
  mensajesEnCola = topicos.get(nombreTopico)
  if mensajesEnCola:
    try:
      return mensajesEnCola.get(block=False)
    except queue.Empty:
      return None

def verTodosLosMensajes(topico):
    mensajesEnCola = topicos.get(topico)
    if mensajesEnCola:
        mensajes = list(mensajesEnCola.queue)  
        return mensajes
    else:
        return None