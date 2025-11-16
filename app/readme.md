# SatLink API - Satellite Resource Management

API para gestión de reservas de recursos satelitales en tiempo real con sincronización entre múltiples usuarios.

## Estructura del Proyecto

```
.
├── main.py                          # Punto de entrada de la aplicación
├── models.py                        # Modelos Pydantic
├── storage.py                       # Repositorios con replicación
├── dependencies.py                  # Dependency injection
├── config.py                        # Configuración
├── requirements.txt                 # Dependencias
├── .env.example                     # Ejemplo de variables de entorno
├── cluster/
│   ├── cluster_manager.py          # Gestión de cluster master-slave
│   └── modification_history.py     # Historial de modificaciones
├── services/
│   ├── notification_service.py     # Servicio de notificaciones
│   ├── window_service.py           # Servicio de ventanas satelitales
│   └── reservation_service.py      # Servicio de reservas
└── routers/
    ├── operators.py                 # Endpoints de operadores
    ├── users.py                     # Endpoints de usuarios
    ├── windows.py                   # Endpoints de ventanas
    ├── reservations.py              # Endpoints de reservas
    └── cluster.py                   # Endpoints de cluster
```

## Instalación

```bash
pip install -r requirements.txt
```

## Configuración

Crea un archivo `.env` basado en `.env.example`:

```bash
cp .env.example .env
```

Variables de configuración:
- `COORDINATOR_URL`: URL del coordinador de cluster (default: http://localhost:9000)
- `NODE_PORT`: Puerto del nodo actual (default: 8000)
- `WRITE_QUORUM`: Número mínimo de nodos que deben confirmar una escritura (default: 2)
- `MODIFICATION_HISTORY_TTL_SECONDS`: Tiempo de vida del historial de modificaciones (default: 3600)

## Ejecución

### Nodo único (sin cluster)

```bash
uvicorn main:app --reload --port 8000
```

### Cluster multi-nodo

Nodo 1:
```bash
NODE_PORT=8001 uvicorn main:app --reload --port 8001
```

Nodo 2:
```bash
NODE_PORT=8002 uvicorn main:app --reload --port 8002
```

Nodo 3:
```bash
NODE_PORT=8003 uvicorn main:app --reload --port 8003
```

La API estará disponible en `http://localhost:8000`

Documentación interactiva: `http://localhost:8000/docs`

## Flujo de Uso

### 1. Crear Operador y Usuario

```bash
# Crear operador
curl -X POST "http://localhost:8000/operators/?name=SatOperator&email=op@example.com"

# Crear usuario
curl -X POST "http://localhost:8000/users/?name=John&email=john@example.com&organization=Research"
```

### 2. Usuario crea alertas

```bash
curl -X POST "http://localhost:8000/users/{user_id}/alerts" \
  -H "Content-Type: application/json" \
  -d '{
    "criteria": {
      "satellite_types": ["optical_observation"],
      "start_date": "2025-11-20T00:00:00Z"
    }
  }'
```

### 3. Operador publica ventana

```bash
curl -X POST "http://localhost:8000/operators/{operator_id}/windows" \
  -H "Content-Type: application/json" \
  -d '{
    "satellite_name": "Sat-1",
    "satellite_type": "optical_observation",
    "resources": ["optical_channel", "radar_channel"],
    "window_datetime": "2025-11-20T10:00:00Z",
    "offer_duration_minutes": 30,
    "location": "South America"
  }'
```

### 4. Usuario reserva ventana

```bash
curl -X POST "http://localhost:8000/reservations/?user_id={user_id}" \
  -H "Content-Type: application/json" \
  -d '{
    "window_id": "{window_id}"
  }'
```

### 5. Usuario selecciona recurso

```bash
curl -X POST "http://localhost:8000/reservations/{reservation_id}/select-resource" \
  -H "Content-Type: application/json" \
  -d '{
    "resource_type": "optical_channel"
  }'
```

### 6. Consultar notificaciones

```bash
curl -X GET "http://localhost:8000/users/{user_id}/notifications"
```

## Características Implementadas

### Arquitectura Master-Slave
- Esquema de replicación con quorum configurable
- Protocolo 2-phase commit (prepare/commit/abort)
- Nodos slave aceptan escrituras del master pero no las commitean hasta confirmación
- Health checks automáticos entre nodos
- Failover mediante historial de modificaciones con TTL configurable

### Gestión de Ventanas Satelitales
- Publicación de ventanas con recursos limitados
- Cierre automático por vencimiento de tiempo de oferta
- Cierre automático cuando todos los recursos están ocupados

### Sistema de Alertas
- Usuarios configuran criterios de búsqueda
- Notificación automática cuando aparece una ventana coincidente

### Reservas en Tiempo Real
- Múltiples usuarios pueden reservar simultáneamente
- Control de concurrencia con locks para selección de recursos
- Sistema de sobre-reserva que permite maximizar ocupación
- Cancelación automática cuando no hay recursos disponibles

### Notificaciones Asíncronas
- Cola de notificaciones procesada en background
- Alertas de nuevas ventanas
- Notificaciones de cancelación
- Avisos de cierre de ventanas

### Manejo de Concurrencia
- Locks granulares por entidad en repositorios
- Lock específico por ventana para asignación de recursos
- Actualización atómica de recursos
- Verificación de disponibilidad en tiempo real
- Sistema de replicación con confirmación de quorum

### Dependency Injection
- Services inyectados en endpoints via FastAPI Depends
- Storage inyectado en todos los endpoints que lo requieren
- Fácil testing y mantenibilidad

## Tipos de Satélites y Recursos

### Tipos de Satélites
- `optical_observation`: Observación óptica
- `radar`: Radar
- `communications`: Comunicaciones
- `experimental`: Experimental

### Tipos de Recursos
- `optical_channel`: Canal óptico
- `radar_channel`: Canal radar
- `data_transmission`: Transmisión de datos
- `sensor_a`: Sensor A
- `sensor_b`: Sensor B

## Estados

### Estado de Ventana
- `open`: Abierta para reservas
- `closed`: Cerrada (vencida o recursos agotados)

### Estado de Reserva
- `pending`: Pendiente de selección de recurso
- `completed`: Completada con recurso seleccionado
- `cancelled`: Cancelada

## Endpoints Principales

### Operadores
- `POST /operators/` - Crear operador
- `GET /operators/{operator_id}` - Obtener operador
- `POST /operators/{operator_id}/windows` - Publicar ventana

### Usuarios
- `POST /users/` - Crear usuario
- `GET /users/{user_id}` - Obtener usuario
- `POST /users/{user_id}/alerts` - Crear alerta
- `GET /users/{user_id}/alerts` - Listar alertas
- `GET /users/{user_id}/notifications` - Obtener notificaciones

### Ventanas
- `GET /windows/` - Listar todas las ventanas
- `GET /windows/{window_id}` - Obtener ventana específica
- `GET /windows/{window_id}/resources` - Ver recursos de ventana
- `GET /windows/{window_id}/available-resources` - Ver recursos disponibles

### Reservas
- `POST /reservations/` - Crear reserva
- `GET /reservations/{reservation_id}` - Obtener reserva
- `GET /reservations/user/{user_id}` - Reservas de usuario
- `POST /reservations/{reservation_id}/select-resource` - Seleccionar recurso
- `POST /reservations/{reservation_id}/cancel` - Cancelar reserva

### Cluster
- `POST /cluster/prepare` - Preparar escritura (2PC fase 1)
- `POST /cluster/commit` - Confirmar escritura (2PC fase 2)
- `POST /cluster/abort` - Abortar escritura
- `GET /health` - Health check del nodo

## Arquitectura de Cluster

### Protocolo de Replicación (2-Phase Commit)

1. **Fase PREPARE**: El master envía la operación a N-1 nodos slave
2. Los slaves guardan la escritura como pendiente y responden ACK
3. **Fase COMMIT**: Si se reciben suficientes ACKs (según WRITE_QUORUM), el master envía COMMIT
4. Los slaves commitean la escritura pendiente
5. **Fase ABORT**: Si faltan ACKs, el master envía ABORT y se descarta la escritura

### Historial de Modificaciones

Cada nodo mantiene un registro ordenado de las últimas modificaciones con:
- Tipo de entidad
- ID de entidad
- Operación realizada
- Datos completos
- Timestamp

Este historial permite:
- Sincronización de nodos tras desconexión
- Recuperación de estado en failover
- Debugging y auditoría

El historial se limpia automáticamente según `MODIFICATION_HISTORY_TTL_SECONDS`.

### Ejemplo de Coordinador

El coordinador debe implementar el endpoint `/cluster/info` que retorna:

```json
{
  "is_master": true,
  "nodes": [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003"
  ]
}
```