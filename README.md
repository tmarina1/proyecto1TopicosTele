# ST0263-4529
## Integrantes
- Tomas Marín Aristizábal, tmarina@eafit.edu.co. 
- Juan Pablo Yepes, jpyepesg@eafit.edu.co.  
- Simón Cárdenas Villada, scardenasv@eafit.edu.co.
## Docente:
- Edwin Nelson Montoya Munera, emontoya@eafit.brightspace.com

# Proyecto #1
## Descripción del proyecto
## 1.1. Que aspectos cumplió o desarrolló de la actividad propuesta por el profesor (requerimientos funcionales y no funcionales)
La comunicación MOM (Message-Oriented Middleware) es una forma de comunicación asíncrona en la que los sistemas se comunican mediante el intercambio de mensajes a través de una plataforma de middleware. 
En la comunicación MOM se deben incluir elementos como: Productores: son los sistemas que generan los mensajes y los envían a la plataforma de middleware, Consumidores: son los sistemas que reciben los mensajes desde la plataforma de middleware y los procesan. Colas: son las estructuras de datos que se utilizan para almacenar los mensajes que se envían entre productores y consumidores. Tópicos: son canales virtuales a través de los cuales se envían mensajes relacionados con un tema o tema específico. En el proyecto se logró realizar un middleware que permite enviar y recibir datos a un conjunto de clientes. Se lograron implementar dos servidores MOM para atacar el requisito de replicación y se añadió un balanceador de cargas usando el principio de Round Robin para atacar el problema de tolerancia a fallas. Además, se logró la interacción asincrónica y la encriptación de mensajes (seguridad).
## 1.2. Que aspectos NO cumplió o desarrolló de la actividad propuesta por el profesor (requerimientos funcionales y no funcionales)  
Faltó implementar el manejo de autenticación de usuarios en el desarrollo del proyecto.  
## 2. información general de diseño de alto nivel, arquitectura, patrones, mejores prácticas utilizadas. 
![arquitecturaProyecto1](https://user-images.githubusercontent.com/61372991/231007707-3559f174-eb54-4346-9b95-685bc9c2504a.png)    

La arquitectura consiste en un Api-Gateway (que genera los request) que esta a la cabeza de dos servidores MOM y por ultimo esta el microservicio. El Api-Gateway esta conectada a los dos MOM y estos están interconectados para sincronizarse, estos MOM están conectados a su vez al microservicio el cual brinda los response.
## 3. Descripción del ambiente de desarrollo y técnico: lenguaje de programación, librerias, paquetes, etc, con sus numeros de versiones.
Todo el proyecto se desarrollo en python. Se usaron librerías como grpc, messages_pb2, messages_pb2_grpc, pickle, base64, entre otras que ayudaron en el proceso del paso de mensajes, de la creación y desarrollo de las colas entre otros.
Ahora bien, algunas de las funciones que se desarrollaron durante el proyecto fueron:  
**En el archivo client.py**  
`roundRobin()`: permite iterar entre los dos servidores MOM.  
`conexionBalanceada()`: permite la conexión entre el API Gateway y los servidores MOM, haciendo uso del principio de balanceo de carga y verificación de errores para garantizar la conexión a un servidor estable.
`conexionCola()`: permite conectarse a la cola dada y brindar una respuesta al servidor MOM.  
`suscribirse()`: permite suscribirse a un tópico.  
`conexionTopico()`: permite conectarse a un tópico dado y brindar una respuesta al servidor MOM.  
**En el archivo mom.py**  
`sync()`: permite sincronizar los datos de los dos servidores MOM.  
`message()`: con ayuda del archivo **messages.proto** dado a continuación 
```bash
syntax = "proto3";

service messageService {
  rpc message (instructionRequest) returns (messageResponse) {}
  rpc sync (instructionRequest) returns (messageResponse) {}
}

message instructionRequest {
  string query = 1;
  bytes estado = 2;
  bool respuesta = 3;
}

message messageResponse {
  repeated string results = 1;
}
```
permite generar los mensajes para cada una de las instrucciones dadas por el Api-Gateway.  
**En el archivo apiGateway.py**   
`roundRobin()` y `conexionBalanceada()`: tienen la misma funcionalidad que en el archivo `client.py`.
Los otros métodos de este archivo son métodos estandarizados para crear una API con la librería FastApi.

   
## 4. Descripción del ambiente de EJECUCIÓN (en producción) lenguaje de programación, librerias, paquetes, etc, con sus numeros de versiones.  
**Maquinas con el proyecto desplegado**  
![WhatsApp Image 2023-04-10 at 4 48 16 PM](https://user-images.githubusercontent.com/61372991/231008586-8a51a37e-91c7-4bbe-8550-8a59eaad43e6.jpeg)  

El proyecto se desplego en AWS. Se deben encender todas las instancias y correr los archivos .py de cada una de ellas.  
Luego, se deben correr los siguientes comandos para el Api Gateway:  
```bash
  cd proyecto1TopicosTele
  python3 apiGateway.py
```
Para el microservicio se deben correr los siguientes comandos: 
```bash
  cd proyecto1TopicosTele/client
  python3 cliente.py
```
Para los servidores MOM se deben correr los siguientes comandos: 
```bash
  cd proyecto1TopicosTele/mom
  python3 mom.py
```
**NOTA:** Como hay dos servidores MOM se debe correr uno con el comando `cd proyecto1TopicosTele/mom` y otro con el comando `cd proyecto1TopicosTele/mom2`  
Luego de esto, se debe buscar en un browser la `ipDelApiGateway:8080/'acción que quiera hacer'` esto es debido a que los servidores MOM ya tienen las ip elasticas por lo cual no se tienen que hacer configuraciones extra. las acciones que se pueden hacer son: **crearCola, agregarElementoCola, listarColas, borrarCola, consumir, crearTopico, agregarMensajeTopico, verMensajesTopico, suscribirTopico, eliminarTopico.**  
- Para crear una cola: `crearCola/'nombreCola'`
- Para borrar un cola: `borrarCola/'nombreCola' ` 
- Para Listar las colas: `listarColas`  
- Para agregar un elemento a la cola: `agregarElementoCola/'nombreCola'/'mensaje' ` 
- Para crear un tópico: `crearTopico/'nombreTopico'`  
- Para borrar un topic: `eliminarTopico/'nombreTopico'`  
- Para agregar elemento en el tópico: `agregarMensajeTopico/'nombreTopico'/'mensaje'`
- Para ver mensajes de un tópico: `verMensajesTopico/'nombreTopico'`  
- Para consumir una cola o tópico: `consumir/ `


## Referencias
* [gRPC](https://grpc.github.io/grpc/python/grpc.html)  
* [gRPC Curso 0263 - Git hub](https://github.com/st0263eafit/st0263-231/tree/main/Laboratorio-RPC)  
* [Uniwebsidad - Métodos de retorno](https://uniwebsidad.com/libros/python/capitulo-8/metodos-de-retorno)  
* [Learn Microsoft - Patrón de publicador y suscriptor](https://learn.microsoft.com/es-es/azure/architecture/patterns/publisher-subscriber)  
* [FastApi](https://fastapi.tiangolo.com/) 
