import asyncio
import pickle
import time
import uuid
from multiprocessing import Process, shared_memory
import struct
from multiprocessing.resource_tracker import unregister  # Importar explícitamente resource_tracker

class CommunicationInterruptedError(Exception):
    """Excepción lanzada cuando la comunicación se interrumpe."""
    pass

class SharedMemoryComm:
    def __init__(self, size=1048576, block_size=100000, timeout=20.0):
        self.size = size
        self.block_size = block_size
        self.timeout = timeout

    def _send_block(self, buf, block: bytes) -> None:
        """Envía un bloque de datos con manejo de timeout."""
        start_time = time.time()
        while buf[4] & 2 != 2:
           # print(f"Emisor: Esperando confirmación de lectura, flags={buf[4]}, tiempo={time.time()-start_time:.2f}s")
            if time.time() - start_time > self.timeout:
                buf[4] |= 4
                raise CommunicationInterruptedError("Timeout esperando confirmación de lectura")
       # print(f"Emisor: Confirmación de lectura recibida, flags={buf[4]}")
        buf[4] &= ~2
        length = len(block)
        buf[5:9] = struct.pack('i', length)
        buf[9:9 + length] = block
       # print(f"Emisor: Enviando bloque de {length} bytes")
        buf[4] |= 1
        start_time = time.time()
        while buf[4] & 2 != 2:
           # print(f"Emisor: Esperando confirmación de lectura tras envío, flags={buf[4]}, tiempo={time.time()-start_time:.2f}s")
            if time.time() - start_time > self.timeout:
                buf[4] |= 4
                raise CommunicationInterruptedError("Timeout esperando confirmación de lectura")
            else:
                time.sleep(0.01)
       # print(f"Emisor: Bloque enviado y confirmado, flags={buf[4]}")

    async def _monitor_interruption(self, buf, task: asyncio.Task, side: str) -> None:
        """Monitorea flags de interrupción y finalización."""
        while True:
            flags = buf[4]
           # print(f"{side}: Monitoreando, flags={flags}")
            if flags & 4:
                task.cancel()
                raise CommunicationInterruptedError(f"Comunicación interrumpida en el {side}")
            if flags & 8:
                break
            await asyncio.sleep(0.01)

    async def send(self, name: str, data: any) -> None:
        """Envía datos serializados por bloques."""
        serialized_data = pickle.dumps(data)
        print(f"Emisor: Tamaño de datos serializados: {len(serialized_data)} bytes")
        blocks = [serialized_data[i:i + self.block_size] for i in range(0, len(serialized_data), self.block_size)]
        total_blocks = len(blocks)
        print(f"Emisor: Total de bloques: {total_blocks}")
        shm = shared_memory.SharedMemory(create=True, size=self.size, name=name)
        buf = shm.buf
        buf[0:4] = struct.pack('i', total_blocks)
        buf[4] = 2  # Flag inicial: "leído"
        
        # Esperar a que el receptor esté listo
        start_time = time.time()
        while buf[4] & 16 != 16:
           # print(f"Emisor: Esperando receptor listo, flags={buf[4]}, tiempo={time.time()-start_time:.2f}s")
            if time.time() - start_time > self.timeout:
                buf[4] |= 4
                shm.close()
                try:
                    unregister(f"/{name}", "shared_memory")  # <-- primero
                    shm.unlink()
                   # print("Emisor: Memoria compartida eliminada")

                except FileNotFoundError:
                   # print("Emisor: La memoria compartida ya fue eliminada")
                   pass
                raise CommunicationInterruptedError("Timeout esperando receptor listo")
            time.sleep(0.005)

        try:
            async def send_task():
                for i, block in enumerate(blocks):
                   # print(f"Emisor: Enviando bloque {i+1}/{total_blocks}")
                    self._send_block(buf, block)
                buf[4] |= 8  # Flag de finalización
               # print("Emisor: Finalización establecida")
            task = asyncio.create_task(send_task())
            monitor = asyncio.create_task(self._monitor_interruption(buf, task, "emisor"))
            await asyncio.gather(task, monitor)
        except asyncio.CancelledError:
            buf[4] |= 4
            raise CommunicationInterruptedError("Envío cancelado por interrupción")
        finally:
            shm.close()
            try:
                shm.unlink()
               # print("Emisor: Memoria compartida eliminada")
                unregister(f"/{name}", "shared_memory")  # Desregistrar explícitamente
            except FileNotFoundError:
               # print("Emisor: La memoria compartida ya fue eliminada")
               pass

    async def receive(self, name: str) -> any:
        """Recibe datos por bloques."""
        shm = None
        start_time = time.time()
        while True:
            try:
                shm = shared_memory.SharedMemory(name=name)
               # print("Receptor: Conectado a la memoria compartida")
                break
            except FileNotFoundError:
                if time.time() - start_time > self.timeout:
                    raise CommunicationInterruptedError("Timeout esperando la creación de la memoria compartida")
                await asyncio.sleep(0.01)
        buf = shm.buf
        buf[4] |= 16  # Señalizar que el receptor está listo
       # print(f"Receptor: Receptor listo, flags={buf[4]}")
        total_blocks = struct.unpack('i', buf[0:4])[0]
       # print(f"Receptor: Total de bloques esperados: {total_blocks}")
        data_blocks = []

        try:
            async def receive_task():
                for i in range(total_blocks):
                    start_time = time.time()
                    while buf[4] & 1 != 1:
                       # print(f"Receptor: Esperando datos listos, flags={buf[4]}, tiempo={time.time()-start_time:.2f}s")
                        if time.time() - start_time > self.timeout:
                            buf[4] |= 4
                            raise CommunicationInterruptedError("Timeout esperando datos")
                        await asyncio.sleep(0.0001)
                    length = struct.unpack('i', buf[5:9])[0]
                    block = buf[9:9 + length].tobytes()
                   # print(f"Receptor: Bloque {i+1}/{total_blocks} recibido de {length} bytes")
                    data_blocks.append(block)
                    buf[4] |= 2
                   # print(f"Receptor: Flag de leído establecido, flags={buf[4]}")
                    buf[4] &= ~1  # Limpiar flag de datos listos
            task = asyncio.create_task(receive_task())
            monitor = asyncio.create_task(self._monitor_interruption(buf, task, "receptor"))
            await asyncio.gather(task, monitor)
            serialized_data = b''.join(data_blocks)
           # print(f"Receptor: Tamaño total de datos recibidos: {len(serialized_data)} bytes")
            return pickle.loads(serialized_data)
        except asyncio.CancelledError:
            buf[4] |= 4
            raise CommunicationInterruptedError("Recepción cancelada por interrupción")
        finally:
            shm.close()
            try:
                unregister(f"/{name}", "shared_memory")  # Desregistrar explícitamente
               # print("Receptor: Memoria compartida eliminada")
                shm.unlink()
                
            except FileNotFoundError:
               # print("Receptor: La memoria compartida ya fue eliminada")
               pass

def run_sender(shm_name,large_data):
    async def sender():
        comm = SharedMemoryComm(size=1048576, block_size=100000, timeout=20.0)
        try:
            await comm.send(shm_name, large_data)
           # print("Datos enviados correctamente.")
        except CommunicationInterruptedError as e:
           # print(f"Error en emisor: {e}")
           pass
    asyncio.run(sender())

def run_receiver(shm_name):
    async def receiver():
        comm = SharedMemoryComm(size=1048576, block_size=100000, timeout=20.0)
        try:
            result = await comm.receive(shm_name)
            print("Datos recibidos:", result)
        except CommunicationInterruptedError as e:
            print(f"Error en receptor: {e}")
    asyncio.run(receiver())

if __name__ == "__main__":
    # Generar un nombre único para la memoria compartida
    shm_name = f"test_shm_{uuid.uuid4().hex[:8]}"
    print(f"Usando nombre de memoria compartida: {shm_name}")
    
    # Verificar tamaño de datos serializados
    large_data = {"key": ("value" * 1000)*10}
    print(f"Tamaño de datos serializados: {len(pickle.dumps(large_data))/(1024*1024)} mb")
    
    receiver_process = Process(target=run_receiver, args=(shm_name,))
    sender_process = Process(target=run_sender, args=(shm_name,large_data))
    receiver_process.start()
    time.sleep(1.0)
    sender_process.start()
    sender_process.join()
    receiver_process.join()
    time.sleep(1.0)  # Retardo adicional para limpieza