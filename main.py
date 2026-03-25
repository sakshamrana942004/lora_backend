import serial.tools.list_ports
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import meshtastic.serial_interface
from pubsub import pub
import asyncio
import socket
from datetime import datetime
import threading

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class MeshtasticManager:
    def __init__(self):
        self.interface = None
        self.active_connections = set()
        self.current_port = None
        self.is_online = True
        self.loop = None # Will be set on startup

    def check_internet_socket(self):
        try:
            socket.setdefaulttimeout(1)
            s = socket.create_connection(("1.1.1.1", 53), 1)
            s.close()
            return True
        except:
            return False

    async def monitor_internet(self):
        while True:
            # Non-blocking check
            self.is_online = await asyncio.to_thread(self.check_internet_socket)
            await asyncio.sleep(10) 

    def auto_connect(self):
        ports = serial.tools.list_ports.comports()
        for p in ports:
            if any(x in p.description for x in ["Silicon Labs", "USB Serial", "CH340", "CP210", "T3S3"]):
                if self.current_port == p.device and self.interface:
                    return {"username": self.get_my_name(), "port": p.device}
                try:
                    # Clean up old connection
                    if self.interface: 
                        self.interface.close()
                    
                    # Connect with a dedicated thread for pubsub
                    self.interface = meshtastic.serial_interface.SerialInterface(devPath=p.device)
                    pub.subscribe(self.on_receive, "meshtastic.receive")
                    self.current_port = p.device
                    return {"username": self.get_my_name(), "port": p.device}
                except: continue
        return None

    def on_receive(self, packet, interface):
        if 'decoded' in packet and packet['decoded'].get('portnum') == 'TEXT_MESSAGE_APP':
            now = datetime.now().strftime("%H:%M:%S")
            data = {
                "text": packet['decoded']['text'],
                "sender": packet.get('fromId', 'Unknown'),
                "via": "LoRa",
                "time": now
            }
            # Push to all active WS clients
            if self.loop:
                for connection in list(self.active_connections):
                    self.loop.call_soon_threadsafe(
                        lambda c=connection, d=data: asyncio.create_task(c.send_json(d))
                    )

    def get_my_name(self):
        if not self.interface: return "Unknown"
        try:
            my_info = self.interface.getMyNodeInfo()
            user = self.interface.nodes.get(my_info.get('num', 0), {}).get('user', {})
            return user.get('longName') or f"Node-{hex(my_info.get('num', 0))[2:]}"
        except: return "Syncing..."

mesh = MeshtasticManager()

@app.on_event("startup")
async def startup_event():
    mesh.loop = asyncio.get_running_loop()
    asyncio.create_task(mesh.monitor_internet())

@app.get("/status")
def get_system_status():
    return {
        "internet": mesh.is_online,
        "hardware": mesh.current_port,
        "username": mesh.get_my_name(),
        "radio": {"freq": "865.875 MHz", "power": "30 dBm"}
    }

@app.get("/auto-scan")
def scan():
    res = mesh.auto_connect()
    return {"status": "success", **res} if res else {"status": "searching"}

# ASYNC SENDING TO REMOVE 7SEC DELAY
def async_send_lora(text, target):
    try:
        if mesh.interface:
            mesh.interface.sendText(text, destinationId=target, wantAck=False)
    except:
        mesh.interface = None

@app.post("/send")
async def send_message(text: str, background_tasks: BackgroundTasks, target: str = "^all"):
    if not mesh.interface: return {"status": "error"}
    
    # Task ko background mein daal do, wait mat karo
    background_tasks.add_task(async_send_lora, text, target)
    
    return {
        "status": "sent", 
        "mode": "Internet" if mesh.is_online else "LoRa",
        "time": datetime.now().strftime("%H:%M:%S")
    }

@app.get("/peers")
def get_peers():
    if not mesh.interface: return []
    peers = []
    try:
        my_id = mesh.interface.getMyNodeInfo().get('num')
        my_hex = f"!{hex(my_id)[2:]}"
        for node_id, info in mesh.interface.nodes.items():
            if 'user' in info and info['user']['id'] != my_hex:
                peers.append({"id": info['user']['id'], "name": info['user'].get('longName', 'Unknown')})
    except: pass
    return peers

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    mesh.active_connections.add(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        mesh.active_connections.remove(websocket)