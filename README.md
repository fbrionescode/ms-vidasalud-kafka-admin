# ms-vidasalud-kafka-admin

Microservicio de administración de Apache Kafka para **VidaSalud** (EP5 / EP6).
Expone endpoints REST utilizando `AdminClient` de Kafka para inspeccionar el clúster, administrar tópicos, modificar parámetros dinámicos y monitorear el **lag** de los consumer groups.

## Puerto y Variables de Entorno

- **Puerto:** `8085`
- **`KAFKA_BOOTSTRAP`:** Servidores bootstrap de Kafka (por defecto `localhost:9092,localhost:9093,localhost:9094` en local; `kafka1:29092,kafka2:29092,kafka3:29092` en Docker).

## Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/kafka/cluster` | Estado del clúster, controlador activo y lista de brokers |
| `GET` | `/api/kafka/topicos` | Lista de tópicos con particiones, réplicas y marca de protección |
| `GET` | `/api/kafka/topicos/{nombre}` | Detalle de particiones (líder, réplicas, ISR) y configuraciones |
| `POST` | `/api/kafka/topicos` | Crea un tópico validando nombre, particiones, réplicas y lista blanca |
| `PATCH` | `/api/kafka/topicos/{nombre}/config` | Modifica en caliente `retention.ms`, `cleanup.policy`, `min.insync.replicas`, `segment.ms` o `max.message.bytes` |
| `DELETE` | `/api/kafka/topicos/{nombre}/config/{clave}` | Elimina una configuración del tópico (vuelve al valor por defecto del broker). Solo claves de la lista blanca; `403 Forbidden` en los tópicos protegidos |
| `POST` | `/api/kafka/topicos/{nombre}/particiones` | Aumenta la cantidad de particiones de un tópico |
| `DELETE` | `/api/kafka/topicos/{nombre}` | Elimina un tópico (retorna `403 Forbidden` si es uno de los 4 tópicos protegidos) |
| `GET` | `/api/kafka/grupos` | Lista todos los consumer groups registrados en el clúster |
| `GET` | `/api/kafka/grupos/{id}` | Detalle de miembros activos, particiones asignadas, offsets y **lag por partición** |