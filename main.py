import serial.tools.list_ports
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import meshtastic.serial_interface
from pubsub import pub
import asyncio
import socket
import time

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
        self.loop = asyncio.get_event_loop()

    # Sabse reliable internet check using socket
    def check_internet_socket(self):
        try:
            # Connect to Cloudflare DNS (1.1.1.1) on port 53
            socket.setdefaulttimeout(2)
            host = socket.gethostbyname("1.1.1.1")
            s = socket.create_connection((host, 53), 2)
            s.close()
            return True
        except:
            return False

    async def monitor_internet(self):
        while True:
            # Running synchronous socket check in a thread to keep FastAPI fast
            self.is_online = await asyncio.to_thread(self.check_internet_socket)
            print(f"DEBUG: Internet is {'ONLINE' if self.is_online else 'OFFLINE'}")
            await asyncio.sleep(5) # Har 5 sec mein check karega

    def auto_connect(self):
        ports = serial.tools.list_ports.comports()
        for p in ports:
            if any(x in p.description for x in ["Silicon Labs", "USB Serial", "CH340", "CP210", "T3S3"]):
                if self.current_port == p.device and self.interface:
                    return {"username": self.get_my_name(), "port": p.device, "status": "active"}
                try:
                    if self.interface: self.interface.close()
                    self.interface = meshtastic.serial_interface.SerialInterface(devPath=p.device)
                    pub.subscribe(self.on_receive, "meshtastic.receive")
                    self.current_port = p.device
                    return {"username": self.get_my_name(), "port": p.device}
                except: continue
        return None

    def get_radio_details(self):
        return {"freq": "865.875 MHz", "power": "30 dBm"}

    def get_my_name(self):
        if not self.interface: return "Unknown"
        try:
            my_info = self.interface.getMyNodeInfo()
            node_id = my_info.get('num')
            user = self.interface.nodes.get(node_id, {}).get('user', {})
            return user.get('longName') or f"Node-{hex(node_id)[2:]}"
        except: return "Syncing..."

    def on_receive(self, packet, interface):
        if 'decoded' in packet and packet['decoded'].get('portnum') == 'TEXT_MESSAGE_APP':
            data = {
                "text": packet['decoded']['text'],
                "sender": packet.get('fromId', 'Unknown'),
                "snr": packet.get('rxSnr', 'N/A'),
                "via": "LoRa"
            }
            for connection in self.active_connections:
                self.loop.create_task(connection.send_json(data))

mesh = MeshtasticManager()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(mesh.monitor_internet())

@app.get("/status")
def get_system_status():
    return {
        "internet": mesh.is_online,
        "hardware": mesh.current_port,
        "username": mesh.get_my_name(),
        "radio": mesh.get_radio_details()
    }

@app.get("/auto-scan")
def scan():
    res = mesh.auto_connect()
    if res: return {"status": "success", **res}
    return {"status": "searching"}

@app.post("/send")
async def send_message(text: str, target: str = "^all"):
    if not mesh.interface: return {"status": "error", "message": "No Device"}
    try:
        mesh.interface.sendText(text, destinationId=target, wantAck=False)
        return {"status": "sent", "mode": "Internet" if mesh.is_online else "LoRa"}
    except:
        mesh.interface = None
        mesh.current_port = None
        return {"status": "error", "message": "Disconnected"}

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
    except: pass
    finally:
        if websocket in mesh.active_connections:
            mesh.active_connections.remove(websocket)