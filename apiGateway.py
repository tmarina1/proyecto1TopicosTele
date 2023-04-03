from fastapi import FastAPI
import uvicorn
import grpc
import messages_pb2
import messages_pb2_grpc
import sys
from google.protobuf.json_format import MessageToDict

#uvicorn apiGateway:app --reload
app = FastAPI()
roundRobin = 0

@app.get("/crearCola/{nombreCola}")
async def root(nombreCola):
  request = f'crearCola/{nombreCola}'
  conexionGRPC = gRPC(request)
  response = conexionGRPC

  return {"Respuesta": response}

@app.get("/borrarCola")
async def root(archivo):
  request = 'borrarCola'
  conexionGRPC = gRPC(request)
  response = conexionGRPC
  
  return {"Respuesta": response}

@app.get("/listarColas")
async def root():
  request = 'listarColas'
  conexionGRPC = gRPC(request)
  response = conexionGRPC
  
  return {"Respuesta": response}

@app.get("/listarElementosCola/{nombreCola}")
async def root(nombreCola):
  request = f'listarElementosCola/{nombreCola}'
  conexionGRPC = gRPC(request)
  response = conexionGRPC
  
  return {"Respuesta": response}

@app.get("/agregarElemento/{nombreCola}/{mensaje}")
async def root(nombreCola, mensaje):
  request = f'agregarElemento/{nombreCola}/{mensaje}'
  conexionGRPC = gRPC(request)
  response = conexionGRPC
  
  return {"Respuesta": response}

@app.get("/verCola/{nombreCola}")
async def root(nombreCola):
  request = f'verCola/{nombreCola}'
  conexionGRPC = gRPC(request)
  response = conexionGRPC
  
  return {"Respuesta": response}

#######################################

@app.get("/crearTopico")
async def root():
  request = 'crearCola'
  conexionGRPC = gRPC(request)
  response = conexionGRPC

  return {"Respuesta": response}

@app.get("/borrarTopico")
async def root(archivo):
  request = 'borrarTopico'
  conexionGRPC = gRPC(request)
  response = conexionGRPC
  
  return {"Respuesta": response}

@app.get("/listarCola")
async def root(archivo):
  request = 'listarCola'
  conexionGRPC = gRPC(request)
  response = conexionGRPC
  
  return {"Respuesta": response}

def gRPC(request):
  channel = grpc.insecure_channel(f'127.0.0.1:8080')
  stub = messages_pb2_grpc.messageServiceStub(channel)
  response = stub.message(messages_pb2.instructionRequest(query=request))
  response  = MessageToDict(response)
  return response 

if __name__ == "__main__":
  uvicorn.run(app, host="127.0.0.1", port=8001)