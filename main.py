from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import os

# Importamos tu clase original (modelo.py)
from modelo import SistemaSalonDB

app = FastAPI(title="Sistema Salon Cloud", version="1.0")

# Conectamos con la base de datos que moviste a esta carpeta
nombre_db = "salon_sistema_pro.db"
if not os.path.exists(nombre_db):
    print(f"ADVERTENCIA: No encuentro {nombre_db}, se creará una nueva vacía.")

db = SistemaSalonDB(nombre_db)

# --- 1. DEFINIR CÓMO SON LOS DATOS (Esquemas) ---
# Esto sirve para que la API sepa qué datos pedir y entregar

class LoginRequest(BaseModel):
    usuario: str
    password: str

class ProductoSchema(BaseModel):
    id: int
    nombre: str
    precio: float
    stock: int

# --- 2. CREAR LAS RUTAS (Puntos de Acceso) ---

@app.get("/")
def home():
    return {"mensaje": "¡Hola Franklin! Tu sistema ya está corriendo en la web 🚀"}

@app.post("/login")
def login(datos: LoginRequest):
    # Usamos tu función original 'validar_login'
    exito = db.validar_login(datos.usuario, datos.password)
    if exito:
        return {"status": "ok", "mensaje": "Bienvenido al sistema"}
    else:
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

@app.get("/productos", response_model=List[ProductoSchema])
def obtener_productos():
    # Usamos tu función original 'traer_productos'
    lista_tuplas = db.traer_productos()
    
    # Convertimos las tuplas de tu DB a Diccionarios para la web
    resultado = []
    for p in lista_tuplas:
        resultado.append({
            "id": p[0],
            "nombre": p[1],
            "precio": p[2],
            "stock": p[3]
        })
    return resultado
# --- Pega esto al final de main.py ---

class NuevoProducto(BaseModel):
    nombre: str
    precio: float
    stock: int

@app.post("/crear-producto")
def crear_producto(datos: NuevoProducto):
    # Usamos tu función original del modelo
    exito, mensaje = db.crear_producto(datos.nombre, datos.precio, datos.stock)
    
    if exito:
        return {"status": "ok", "mensaje": mensaje}
    else:
        raise HTTPException(status_code=400, detail=mensaje)
# --- PEGAR AL FINAL DE main.py ---

# 1. Esquema de datos para una Cita
class CitaInput(BaseModel):
    cliente: str
    telefono: str
    profesional: str
    servicio: str
    fecha: str  # Formato YYYY-MM-DD
    hora: str   # Formato HH:MM

# 2. Endpoint para AGENDAR (Crear)
@app.post("/agendar")
def agendar_cita(cita: CitaInput):
    # A. Buscamos info del servicio (precio y duración)
    info_srv = db.get_info_servicio(cita.servicio)
    if not info_srv:
        raise HTTPException(status_code=400, detail="El servicio no existe")
    
    id_srv, duracion, precio = info_srv

    # B. Validamos si hay choque de horario (¡Usando tu lógica original!)
    ocupado, fin_msg = db.validar_choque(cita.fecha, cita.hora, duracion, cita.profesional)
    
    if ocupado:
        raise HTTPException(status_code=400, detail=f"Ocupado: {fin_msg}")

    # C. Preparamos el paquete para guardar (tu modelo espera una lista 'carrito')
    item_cita = {
        'servicio': cita.servicio,
        'profesional': cita.profesional,
        'inicio': cita.hora,
        'fin': fin_msg,
        'fecha': cita.fecha,
        'precio': precio
    }
    
    # D. Guardamos usando tu función maestra
    ok, msg = db.guardar_paquete_citas([item_cita], cita.cliente, cita.telefono)
    
    if ok:
        return {"status": "ok", "mensaje": f"Cita Agendada: {msg}"}
    else:
        raise HTTPException(status_code=400, detail=msg)

# 3. Endpoint para LISTAR (Ver Agenda)
@app.get("/citas")
def listar_citas():
    # Usamos tu función que trae la agenda filtrada
    filas = db.traer_agenda_filtrada()
    
    lista_json = []
    # Convertimos las tuplas a JSON: (id, fecha, hora, cli, tel, srv, pro, est)
    for r in filas:
        lista_json.append({
            "id": r[0],
            "fecha": r[1],
            "hora": r[2],
            "cliente": r[3],
            "servicio": r[5],
            "profesional": r[6],
            "estado": r[7]
        })
    return lista_json

# 4. Auxiliar: Listas para llenar los Combobox (Desplegables)
@app.get("/listas-agenda")
def obtener_listas():
    pros, servs = db.get_listas()
    return {"profesionales": pros, "servicios": servs}