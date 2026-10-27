"""
PySwarm — universal load testing tool.

Modes:
    python pyswarm.py                                 # local: backend + master + N workers
    python pyswarm.py --master                        # distributed master: backend + master only
    python pyswarm.py --worker --master-host HOST     # worker: connect to remote master

Options:
    --master-host HOST      RPC host of master (default 127.0.0.1)
    --master-rpc-port PORT  RPC port of master (default 5557)
    --master-url URL        HTTP URL of master for pulling bundle (default http://HOST:8000)

Env:
    LOCUST_WORKERS=N        local workers in default mode (default 1)
"""

import asyncio
import json
import logging
import multiprocessing
import os
import random
import socket
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import httpx
import psutil
import uvicorn
from fastapi import (
    FastAPI,
    File,
    HTTPException,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pyswarm")

# ---------- constants ----------
LOCUST_HOST = "127.0.0.1"
LOCUST_HTTP_PORT = 8089
LOCUST_RPC_PORT = 5557
BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8000

MAX_SYSTEM_MEMORY_PERCENT = 95
MAX_LOCUST_PROCESS_MEM_MB = 2048
MAX_UPLOAD_SIZE_MB = 50

# Filled from argv in main()
MASTER_RPC_HOST = "127.0.0.1"
MASTER_RPC_PORT = LOCUST_RPC_PORT
MASTER_HTTP_URL: Optional[str] = None
IS_WORKER_MODE = False

if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TMP_DIR = os.path.join(BASE_DIR, "tmp")


# ---------- embedded static ----------
INDEX_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>PySwarm Dashboard</title>
    <link rel="stylesheet" href="/style.css?v=3">
    <script src="/chart.js"></script>
</head>
<body>
    <div class="controls">
        <div class="controls-row">
            <div class="control-block">
                <label>Пользователи (Нагрузка)</label>
                <div class="input-wrapper">
                    <input type="range" id="usersRange">
                    <input type="number" id="usersInput" class="styled-input" style="width: 60px; text-align: center;">
                </div>
            </div>
            <div class="control-block">
                <label>Скорость (юзеров/сек)</label>
                <div class="input-wrapper">
                    <input type="range" id="spawnRange">
                    <input type="number" id="spawnInput" class="styled-input" style="width: 60px; text-align: center;">
                </div>
            </div>
            <div class="control-block">
                <label>Длительность (сек)</label>
                <div class="input-wrapper">
                    <input type="range" id="durationRange">
                    <input type="number" id="durationInput" class="styled-input" style="width: 60px; text-align: center;">
                </div>
            </div>
            <div class="control-block">
                <label>Задержка (сек)</label>
                <div class="input-wrapper">
                    <input type="range" id="waitRange">
                    <input type="number" id="waitInput" class="styled-input" style="width: 60px; text-align: center;" step="0.1">
                </div>
            </div>
        </div>

        <div class="controls-row">
            <div class="control-block">
                <label>Целевой URL</label>
                <input type="text" id="hostInput" value="http://127.0.0.1:8080" class="styled-input" style="width: 350px;">
            </div>
            <div class="control-block">
                <label>Bearer Токен (опционально)</label>
                <input type="text" id="tokenInput" placeholder="Вставьте токен..." class="styled-input" style="width: 350px;">
            </div>
            <div class="control-block">
                <label>Режим контура</label>
                <div style="display: flex; align-items: center; height: 38px;">
                    <button type="button" id="loopModeBtn" class="btn-closed" onclick="toggleLoopMode()">Закрытый контур</button>
                </div>
            </div>
        </div>

        <div class="controls-row">
            <div class="control-block">
                <label>Папка с JSON</label>
                <div style="display: flex; align-items: center; gap: 15px;">
                    <input type="file" id="folderInput" webkitdirectory directory multiple class="styled-input" style="width: 220px; font-size: 13px;">
                    <span id="uploadStatus" style="font-size: 14px; color: #4CAF50;"></span>
                </div>
            </div>
            <div class="control-block">
                <button onclick="startTest()" id="startBtn" style="padding: 10px 30px;">Запустить тест</button>
                <button onclick="stopTest()" id="stopBtn" style="background: #F44336; padding: 10px 30px; display: none;">Остановить</button>
            </div>
        </div>

        <div id="status" style="text-align: center; font-weight: bold; color: #ff9800; font-size: 18px; margin-top: 10px;">Статус: Ожидание</div>
    </div>
    <div class="container">
        <div class="card">
            <h2>RPS (Запросы в секунду)</h2>
            <canvas id="rpsChart"></canvas>
        </div>
        <div class="card">
            <h2>Пользователи / Ошибки</h2>
            <canvas id="usersChart"></canvas>
        </div>
    </div>
    <div class="container" style="margin-top: 20px;">
        <div class="card" style="width: 95%;">
            <h2 style="display: flex; justify-content: space-between; align-items: center;">
                <span>Детальная статистика по эндпоинтам</span>
                <span id="cpuLoad" style="color: #ff9800; font-size: 14px; margin: 0;">CPU: 0%</span>
            </h2>
            <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 14px;">
                <thead>
                    <tr style="border-bottom: 2px solid #555; color: #bbb;">
                        <th style="padding: 10px;">Метод</th>
                        <th style="padding: 10px;">Эндпоинт</th>
                        <th style="padding: 10px; color: #4CAF50;">Среднее</th>
                        <th style="padding: 10px;">p50</th>
                        <th style="padding: 10px;">p95</th>
                        <th style="padding: 10px;">p99</th>
                    </tr>
                </thead>
                <tbody id="statsTableBody"></tbody>
            </table>
        </div>
    </div>
    <script src="/script.js"></script>
</body>
</html>"""

STYLE_CSS = """body { font-family: sans-serif; background: #1e1e1e; color: #fff; margin: 20px; }
.container { display: flex; flex-wrap: wrap; gap: 20px; justify-content: center; }
.card { background: #2d2d2d; padding: 20px; border-radius: 8px; width: 45%; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
h2 { text-align: center; font-size: 18px; margin-bottom: 15px; color: #4CAF50; }
.controls { text-align: center; margin-bottom: 20px; }
button { padding: 10px 20px; font-size: 16px; background: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; }
button:hover { background: #45a049; }
#status { text-align: center; margin-top: 10px; font-weight: bold; color: #ff9800; }
.input-wrapper { display: flex; align-items: center; gap: 10px; }
input[type="range"] { cursor: pointer; accent-color: #4CAF50; }
input[type="number"] { width: 60px; padding: 5px; background: #333; color: #fff; border: 1px solid #555; border-radius: 4px; text-align: center; }
.controls-row { display: flex; justify-content: center; align-items: flex-end; gap: 30px; margin-bottom: 20px; flex-wrap: wrap; }
.control-block { display: flex; flex-direction: column; align-items: center; }
.control-block label { font-size: 14px; color: #bbb; margin-bottom: 8px; }
.styled-input { padding: 8px; background: #333; color: #fff; border: 1px solid #555; border-radius: 4px; }
#loopModeBtn { width: 180px; height: 38px; padding: 0; box-sizing: border-box; display: flex; justify-content: center; align-items: center; white-space: nowrap; font-weight: bold; font-size: 14px; border-radius: 6px; border: 2px solid transparent; transition: all 0.2s ease-in-out; color: #fff; cursor: pointer; }
#loopModeBtn.btn-closed { background-color: #2196F3 !important; border-color: #1976D2 !important; }
#loopModeBtn.btn-closed:hover { background-color: #1E88E5 !important; box-shadow: 0 0 10px rgba(33, 150, 243, 0.4); }
#loopModeBtn.btn-open { background-color: #ff9800 !important; border-color: #f57c00 !important; }
#loopModeBtn.btn-open:hover { background-color: #fb8c00 !important; box-shadow: 0 0 10px rgba(255, 152, 0, 0.4); }"""

CHART_JS_PATH = os.path.join(BASE_DIR, "chart.min.js")
if os.path.exists(CHART_JS_PATH):
    with open(CHART_JS_PATH, "r", encoding="utf-8") as f:
        CHART_JS = f.read()
else:
    logger.error("Файл chart.min.js не найден рядом с pyswarm.py!")
    CHART_JS = "console.error('Chart.js not found!');"

SCRIPT_JS = """const API_BASE = window.location.origin;
const WS_URL = (window.location.protocol === 'https:' ? 'wss://' : 'ws://') + window.location.host + '/ws/metrics';

let isOpenLoop = false;
let ws = null;
let testTimer = null;
let manualStop = false;

const usersRange = document.getElementById('usersRange');
const usersInput = document.getElementById('usersInput');
const spawnRange = document.getElementById('spawnRange');
const spawnInput = document.getElementById('spawnInput');
const durationRange = document.getElementById('durationRange');
const durationInput = document.getElementById('durationInput');
const waitRange = document.getElementById('waitRange');
const waitInput = document.getElementById('waitInput');

usersRange.addEventListener('input', () => { usersInput.value = usersRange.value; });
usersInput.addEventListener('input', () => { usersRange.value = usersInput.value; });
spawnRange.addEventListener('input', () => { spawnInput.value = spawnRange.value; });
spawnInput.addEventListener('input', () => { spawnRange.value = spawnInput.value; });
durationRange.addEventListener('input', () => { durationInput.value = durationRange.value; });
durationInput.addEventListener('input', () => { durationRange.value = durationInput.value; });
waitRange.addEventListener('input', () => { waitInput.value = waitRange.value; });
waitInput.addEventListener('input', () => { waitRange.value = waitInput.value; });

async function loadConfig() {
    try {
        const response = await fetch(API_BASE + '/api/config');
        const config = await response.json();
        usersRange.min = usersInput.min = config.users.min;
        usersRange.max = usersInput.max = config.users.max;
        usersRange.step = usersInput.step = config.users.step;
        usersRange.value = usersInput.value = config.users.default;
        spawnRange.min = spawnInput.min = config.spawn.min;
        spawnRange.max = spawnInput.max = config.spawn.max;
        spawnRange.step = spawnInput.step = config.spawn.step;
        spawnRange.value = spawnInput.value = config.spawn.default;
        durationRange.min = durationInput.min = config.duration.min;
        durationRange.max = durationInput.max = config.duration.max;
        durationRange.step = durationInput.step = config.duration.step;
        durationRange.value = durationInput.value = config.duration.default;
        waitRange.min = waitInput.min = config.wait.min;
        waitRange.max = waitInput.max = config.wait.max;
        waitRange.step = waitInput.step = config.wait.step;
        waitRange.value = waitInput.value = config.wait.default;
    } catch (error) {
        console.error("Config load failed:", error);
    }
}
loadConfig();

const commonOptions = {
    responsive: true,
    animation: false,
    scales: {
        x: { display: false },
        y: { beginAtZero: true, grid: { color: '#444' }, ticks: { color: '#ccc' } }
    },
    plugins: { legend: { labels: { color: '#ccc' } } }
};

const rpsChart = new Chart(document.getElementById('rpsChart'), {
    type: 'line',
    data: { labels: [], datasets: [{ label: 'RPS', data: [], borderColor: '#4CAF50', backgroundColor: 'rgba(76, 175, 80, 0.2)', fill: true, tension: 0.3, pointRadius: 0 }] },
    options: commonOptions
});

const usersChart = new Chart(document.getElementById('usersChart'), {
    type: 'line',
    data: { labels: [], datasets: [
        { label: 'Пользователи', data: [], borderColor: '#2196F3', tension: 0.3, pointRadius: 0 },
        { label: 'Ошибки', data: [], borderColor: '#F44336', tension: 0.3, pointRadius: 0 }
    ]},
    options: commonOptions
});

function setStatus(text, color) {
    const el = document.getElementById('status');
    el.innerText = text;
    if (color) el.style.color = color;
}
function showStartButton() {
    document.getElementById('startBtn').style.display = 'inline-block';
    document.getElementById('stopBtn').style.display = 'none';
}
function showStopButton() {
    document.getElementById('startBtn').style.display = 'none';
    document.getElementById('stopBtn').style.display = 'inline-block';
}
function resetCharts() {
    rpsChart.data.labels = [];
    rpsChart.data.datasets[0].data = [];
    usersChart.data.labels = [];
    usersChart.data.datasets[0].data = [];
    usersChart.data.datasets[1].data = [];
    rpsChart.update();
    usersChart.update();
}
function toggleLoopMode() {
    isOpenLoop = !isOpenLoop;
    const btn = document.getElementById('loopModeBtn');
    btn.innerText = isOpenLoop ? "Открытый контур" : "Закрытый контур";
    btn.className = isOpenLoop ? "btn-open" : "btn-closed";
}

async function startTest() {
    setStatus("Статус: Запуск Locust...");
    manualStop = false;
    if (testTimer) { clearTimeout(testTimer); testTimer = null; }

    const users = parseInt(usersInput.value);
    const spawnRate = parseInt(spawnInput.value);
    const duration = parseInt(durationInput.value);
    const wait_time = parseFloat(waitInput.value);
    const token = document.getElementById('tokenInput').value.trim();
    const host = document.getElementById('hostInput').value.trim();
    const is_open_loop = isOpenLoop;

    try {
        const response = await fetch(API_BASE + '/api/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ users, spawn_rate: spawnRate, host, duration, wait_time, token, is_open_loop })
        });
        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `HTTP ${response.status}`);
        }
        setStatus(`Статус: Тест идёт (${duration} сек)...`);
        showStopButton();
        resetCharts();
        connectWebSocket();
        testTimer = setTimeout(() => {
            if (!manualStop) {
                setStatus("Статус: Тест завершён", "#ff9800");
                showStartButton();
                if (ws) { ws.onclose = null; ws.close(); ws = null; }
            }
        }, (duration + 2) * 1000);
    } catch (error) {
        setStatus("Ошибка: " + error.message, "#F44336");
        showStartButton();
    }
}

async function stopTest() {
    manualStop = true;
    if (testTimer) { clearTimeout(testTimer); testTimer = null; }
    try {
        const response = await fetch(API_BASE + '/api/stop', { method: 'POST' });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        setStatus("Статус: Тест остановлен (вручную)");
    } catch (error) {
        setStatus("Ошибка остановки: " + error.message, "#F44336");
    }
    showStartButton();
    if (ws) { ws.onclose = null; ws.close(); ws = null; }
}

function connectWebSocket() {
    if (ws) { ws.onclose = null; ws.close(); }
    ws = new WebSocket(WS_URL);

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);

        if (data.error_msg) {
            setStatus(data.error_msg, "#F44336");
            manualStop = true;
            if (testTimer) { clearTimeout(testTimer); testTimer = null; }
            showStartButton();
            if (ws) { ws.onclose = null; ws.close(); ws = null; }
            return;
        }

        rpsChart.data.labels.push(data.timestamp);
        rpsChart.data.datasets[0].data.push(parseFloat(data.total_rps) || 0);
        usersChart.data.labels.push(data.timestamp);
        usersChart.data.datasets[0].data.push(parseInt(data.user_count) || 0);
        usersChart.data.datasets[1].data.push(parseInt(data.fails) || 0);

        if (rpsChart.data.labels.length > 30) {
            rpsChart.data.labels.shift();
            rpsChart.data.datasets[0].data.shift();
            usersChart.data.labels.shift();
            usersChart.data.datasets[0].data.shift();
            usersChart.data.datasets[1].data.shift();
        }
        rpsChart.update();
        usersChart.update();

        if (data.cpu_load !== undefined) {
            document.getElementById('cpuLoad').innerText = `CPU: ${data.cpu_load}% | RAM: ${data.mem_load}%`;
        }

        if (data.stats_table) {
            const tbody = document.getElementById('statsTableBody');
            tbody.innerHTML = '';
            data.stats_table.forEach(stat => {
                const tr = document.createElement('tr');
                tr.style.borderBottom = '1px solid #444';
                tr.innerHTML = `
                    <td style="padding: 8px; font-weight: bold; color: ${stat.method === 'GET' ? '#4CAF50' : '#2196F3'};">${stat.method}</td>
                    <td style="padding: 8px;">${stat.name}</td>
                    <td style="padding: 8px; color: #4CAF50;">${stat.avg} ms</td>
                    <td style="padding: 8px;">${stat.p50} ms</td>
                    <td style="padding: 8px;">${stat.p95} ms</td>
                    <td style="padding: 8px;">${stat.p99} ms</td>
                `;
                tbody.appendChild(tr);
            });
        }
    };

    ws.onclose = (evt) => {
        console.log("WS closed", evt.code, evt.reason);
        if (!manualStop) {
            setStatus("Статус: Тест завершён");
            showStartButton();
        }
    };
    ws.onerror = (err) => console.log("WS error:", err);
}

document.getElementById('folderInput').addEventListener('change', async (event) => {
    const files = Array.from(event.target.files);
    if (!files.length) return;
    const jsonFiles = files.filter(f => f.name.endsWith('.json'));
    if (!jsonFiles.length) {
        document.getElementById('uploadStatus').innerText = "❌ В папке нет .json файлов";
        document.getElementById('uploadStatus').style.color = "#F44336";
        return;
    }
    const formData = new FormData();
    jsonFiles.forEach(file => formData.append("files", file));
    const statusEl = document.getElementById('uploadStatus');
    statusEl.innerText = `⏳ Загрузка ${jsonFiles.length} файлов...`;
    statusEl.style.color = "#ff9800";
    try {
        const response = await fetch(API_BASE + '/api/upload_folder', { method: 'POST', body: formData });
        const data = await response.json();
        if (response.ok) {
            let msg = `✅ Файлов: ${data.saved_files}, запросов: ${data.total_requests}`;
            if (data.skipped_files > 0) msg += ` (пропущено: ${data.skipped_files})`;
            statusEl.innerText = msg;
            statusEl.style.color = "#4CAF50";
        } else {
            statusEl.innerText = `❌ Ошибка: ${data.detail || 'неизвестная'}`;
            statusEl.style.color = "#F44336";
        }
    } catch (error) {
        statusEl.innerText = `❌ Ошибка соединения`;
        statusEl.style.color = "#F44336";
    }
});"""


# ---------- Locust env factory ----------
def _create_env():
    """Создаёт Environment с PySwarmUser и обработчиком test_start."""
    from locust import FastHttpUser, constant, constant_throughput, events, task
    from locust.env import Environment

    REQUESTS: List[dict] = []

    class PySwarmUser(FastHttpUser):
        wait_time = constant(1.0)

        @task
        def dynamic_request(self):
            if not REQUESTS:
                return
            req = random.choice(REQUESTS)
            self.client.request(
                method=req.get("method", "GET"),
                url=req.get("url", "/"),
                headers=req.get("headers", {}) or {},
                json=req.get("body"),
            )

    env = Environment(user_classes=[PySwarmUser], events=events)

    @events.test_start.add_listener
    def on_test_start(environment, **kwargs):
        test_data_path = os.path.join(TMP_DIR, "test_data.json")
        config_path = os.path.join(TMP_DIR, "config.json")

        test_data = None
        config = None

        # try local files first
        if os.path.exists(test_data_path):
            try:
                with open(test_data_path, encoding="utf-8") as f:
                    test_data = json.load(f)
            except Exception as e:
                logger.error("Failed to read local test_data: %s", e)
        if os.path.exists(config_path):
            try:
                with open(config_path, encoding="utf-8") as f:
                    config = json.load(f)
            except Exception as e:
                logger.error("Failed to read local config: %s", e)

        # fall back to master bundle (for remote workers)
        if test_data is None and MASTER_HTTP_URL:
            try:
                r = httpx.get(f"{MASTER_HTTP_URL}/api/worker-bundle", timeout=15.0)
                r.raise_for_status()
                bundle = r.json()
                test_data = bundle.get("test_data")
                if config is None:
                    config = bundle.get("config")
                # cache locally
                os.makedirs(TMP_DIR, exist_ok=True)
                if test_data is not None:
                    with open(test_data_path, "w", encoding="utf-8") as f:
                        json.dump(test_data, f, ensure_ascii=False)
                if config is not None:
                    with open(config_path, "w", encoding="utf-8") as f:
                        json.dump(config, f)
                logger.info(
                    "Worker: fetched bundle from master (%d requests)",
                    len(test_data or []),
                )
            except Exception as e:
                logger.error("Worker: failed to fetch bundle from master: %s", e)

        if not test_data:
            logger.error("Locust: no test_data available!")
            return

        REQUESTS.clear()
        REQUESTS.extend(test_data)
        logger.info("Locust: %d requests loaded", len(REQUESTS))

        if config:
            token = config.get("token")
            if config.get("is_open_loop"):
                PySwarmUser.wait_time = constant_throughput(1.0)
            else:
                PySwarmUser.wait_time = constant(config.get("wait_time", 1.0))
            if token:
                for r in REQUESTS:
                    r.setdefault("headers", {})["Authorization"] = f"Bearer {token}"

    return env


def run_master_process():
    env = _create_env()
    env.create_master_runner(
        master_bind_host=LOCUST_HOST,
        master_bind_port=LOCUST_RPC_PORT,
    )
    env.create_web_ui(LOCUST_HOST, LOCUST_HTTP_PORT)
    logger.info(
        "Locust master: UI http://%s:%d, RPC %d",
        LOCUST_HOST,
        LOCUST_HTTP_PORT,
        LOCUST_RPC_PORT,
    )
    env.web_ui.greenlet.join()


def run_worker_process():
    env = _create_env()
    env.create_worker_runner(master_host=LOCUST_HOST, master_port=LOCUST_RPC_PORT)
    logger.info("Locust worker → %s:%d", LOCUST_HOST, LOCUST_RPC_PORT)
    env.runner.greenlet.join()


# ---------- FastAPI ----------
class TestConfig(BaseModel):
    users: int = Field(default=10, ge=1, le=2000)
    spawn_rate: int = Field(default=1, ge=1, le=500)
    host: str = "http://127.0.0.1:8080"
    duration: int = Field(default=30, ge=5, le=7200)
    wait_time: float = Field(default=1.0, ge=0.0, le=10.0)
    token: Optional[str] = None
    is_open_loop: bool = False


class ApiRequest(BaseModel):
    method: str
    url: str
    headers: Optional[Dict[str, str]] = None
    body: Optional[Dict[str, Any]] = None


MEMORY_CRITICAL = False


def _ensure_tmp():
    os.makedirs(TMP_DIR, exist_ok=True)


def _wait_port(host: str, port: int, timeout: float = 10.0) -> bool:
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=0.3)
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global MEMORY_CRITICAL
    MEMORY_CRITICAL = False
    _ensure_tmp()
    logger.info("tmp dir: %s", TMP_DIR)

    # 1. start master
    master_proc = multiprocessing.Process(target=run_master_process, daemon=True)
    master_proc.start()

    if not _wait_port(LOCUST_HOST, LOCUST_RPC_PORT, timeout=15.0):
        logger.error("Locust master RPC did not start")

    # 2. start local workers (0 in --master mode)
    n_local = int(os.getenv("LOCUST_WORKERS", "1"))
    worker_procs: List[multiprocessing.Process] = []
    for _ in range(n_local):
        p = multiprocessing.Process(target=run_worker_process, daemon=True)
        p.start()
        worker_procs.append(p)
    logger.info("Local workers started: %d", n_local)

    # 3. wait for web UI
    for _ in range(100):
        try:
            async with httpx.AsyncClient(trust_env=False) as c:
                r = await c.get(
                    f"http://{LOCUST_HOST}:{LOCUST_HTTP_PORT}/stats/requests",
                    timeout=0.3,
                )
                if r.status_code == 200:
                    logger.info("Locust UI ready")
                    break
        except Exception:
            pass
        await asyncio.sleep(0.1)

    # 4. memory watchdog
    all_locust_procs = [master_proc, *worker_procs]

    async def memory_watchdog():
        global MEMORY_CRITICAL
        while True:
            try:
                sys_mem = psutil.virtual_memory()
                rss_mb = 0.0
                for p in all_locust_procs:
                    try:
                        rss_mb += psutil.Process(p.pid).memory_info().rss / 1024 / 1024
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                if (
                    sys_mem.percent >= MAX_SYSTEM_MEMORY_PERCENT
                    or rss_mb > MAX_LOCUST_PROCESS_MEM_MB
                ):
                    logger.warning(
                        "WATCHDOG: sys=%d%%, locust=%.0f MB", sys_mem.percent, rss_mb
                    )
                    MEMORY_CRITICAL = True
                    try:
                        async with httpx.AsyncClient(trust_env=False) as client:
                            await client.post(
                                f"http://{LOCUST_HOST}:{LOCUST_HTTP_PORT}/stop",
                                timeout=3.0,
                            )
                    except Exception:
                        pass
            except Exception:
                pass
            await asyncio.sleep(2)

    watchdog_task = asyncio.create_task(memory_watchdog())

    yield

    watchdog_task.cancel()
    for p in worker_procs:
        if p.is_alive():
            p.terminate()
    if master_proc.is_alive():
        master_proc.terminate()


app = FastAPI(title="PySwarm", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML


@app.get("/style.css")
async def style():
    return Response(STYLE_CSS, media_type="text/css")


@app.get("/script.js")
async def script():
    return Response(SCRIPT_JS, media_type="application/javascript")


@app.get("/chart.js")
async def chart_js_script():
    return Response(CHART_JS, media_type="application/javascript")


@app.get("/api/worker-bundle")
async def worker_bundle():
    """Master hands out test_data + config to remote workers."""
    bundle: Dict[str, Any] = {}
    td = os.path.join(TMP_DIR, "test_data.json")
    cfg = os.path.join(TMP_DIR, "config.json")
    if os.path.exists(td):
        with open(td, encoding="utf-8") as f:
            bundle["test_data"] = json.load(f)
    if os.path.exists(cfg):
        with open(cfg, encoding="utf-8") as f:
            bundle["config"] = json.load(f)
    if not bundle:
        raise HTTPException(404, "No test data uploaded on master")
    return bundle


@app.post("/api/upload_folder")
async def upload_folder(files: List[UploadFile] = File(...)):
    _ensure_tmp()
    all_requests, results = [], []
    max_bytes = MAX_UPLOAD_SIZE_MB * 1024 * 1024

    for file in files:
        name = os.path.basename(file.filename or "unknown.json")
        if not name.endswith(".json"):
            results.append({"file": name, "status": "skipped", "reason": "not .json"})
            continue
        try:
            contents = await file.read()
            if len(contents) > max_bytes:
                results.append(
                    {
                        "file": name,
                        "status": "skipped",
                        "reason": f"> {MAX_UPLOAD_SIZE_MB}MB",
                    }
                )
                continue
            data = json.loads(contents)
        except json.JSONDecodeError as e:
            results.append(
                {"file": name, "status": "skipped", "reason": f"bad JSON: {e}"}
            )
            continue
        except Exception as e:
            results.append({"file": name, "status": "skipped", "reason": str(e)})
            continue
        if not isinstance(data, list):
            results.append({"file": name, "status": "skipped", "reason": "not a list"})
            continue
        try:
            [ApiRequest(**it) for it in data]
        except Exception as e:
            results.append(
                {"file": name, "status": "skipped", "reason": f"invalid: {e}"}
            )
            continue
        all_requests.extend(data)
        results.append({"file": name, "status": "ok", "count": len(data)})

    if not all_requests:
        return {
            "saved_files": 0,
            "skipped_files": len(results),
            "total_requests": 0,
            "files": results,
        }

    with open(os.path.join(TMP_DIR, "test_data.json"), "w", encoding="utf-8") as f:
        json.dump(all_requests, f, ensure_ascii=False, indent=2)

    saved = sum(1 for r in results if r["status"] == "ok")
    return {
        "saved_files": saved,
        "skipped_files": len(results) - saved,
        "total_requests": len(all_requests),
        "files": results,
    }


@app.post("/api/start")
async def start_load_test(config: TestConfig):
    global MEMORY_CRITICAL
    MEMORY_CRITICAL = False
    _ensure_tmp()

    if not os.path.exists(os.path.join(TMP_DIR, "test_data.json")):
        raise HTTPException(status_code=400, detail="No test_data.json uploaded")

    with open(os.path.join(TMP_DIR, "config.json"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "token": config.token,
                "wait_time": config.wait_time,
                "is_open_loop": config.is_open_loop,
            },
            f,
        )

    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            await client.post(
                f"http://{LOCUST_HOST}:{LOCUST_HTTP_PORT}/stats/reset", timeout=2.0
            )
            r = await client.post(
                f"http://{LOCUST_HOST}:{LOCUST_HTTP_PORT}/swarm",
                data={
                    "user_count": config.users,
                    "spawn_rate": config.spawn_rate,
                    "host": config.host,
                    "run_time": f"{config.duration}s",
                },
                timeout=5.0,
            )
            r.raise_for_status()
        return {"status": "started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Locust: {e}")


@app.post("/api/stop")
async def stop_load_test():
    try:
        async with httpx.AsyncClient(trust_env=False) as client:
            await client.post(
                f"http://{LOCUST_HOST}:{LOCUST_HTTP_PORT}/stop", timeout=3.0
            )
        return {"status": "stopped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_latest_metrics() -> Optional[Dict[str, Any]]:
    global MEMORY_CRITICAL
    try:
        cpu_load = psutil.cpu_percent()
        mem_load = psutil.virtual_memory().percent

        if MEMORY_CRITICAL:
            return {"error_msg": "🛑 Тест остановлен: превышен лимит памяти"}

        async with httpx.AsyncClient(trust_env=False) as client:
            r = await client.get(
                f"http://{LOCUST_HOST}:{LOCUST_HTTP_PORT}/stats/requests", timeout=3.0
            )
            r.raise_for_status()
            data = r.json()

        if data.get("state") not in ("running", "spawning", "ready"):
            return None

        total_fails = data.get("stats_total", {}).get("num_failures", 0)

        stats_table = []
        for s in data.get("stats", []):
            if s.get("name") == "Aggregated":
                continue
            stats_table.append(
                {
                    "name": s.get("name", ""),
                    "method": s.get("method", "GET"),
                    "avg": round(s.get("avg_response_time", 0), 1),
                    "p50": round(s.get("median_response_time", 0)),
                    "p95": s.get("response_time_percentile_0.95", 0),
                    "p99": s.get("response_time_percentile_0.99", 0),
                }
            )

        return {
            "timestamp": str(int(asyncio.get_running_loop().time())),
            "user_count": str(data.get("user_count", 0)),
            "total_rps": str(round(data.get("total_rps", 0), 2)),
            "fails": str(total_fails),
            "cpu_load": cpu_load,
            "mem_load": mem_load,
            "stats_table": stats_table,
        }
    except Exception as e:
        logger.error("metrics: %s", e)
        return None


@app.websocket("/ws/metrics")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            m = await get_latest_metrics()
            if m:
                await websocket.send_json(m)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass


@app.get("/api/config")
async def get_ui_config():
    return {
        "users": {"min": 1, "max": 2000, "step": 1, "default": 20},
        "spawn": {"min": 1, "max": 500, "step": 1, "default": 2},
        "duration": {"min": 5, "max": 7200, "step": 5, "default": 30},
        "wait": {"min": 0.0, "max": 10.0, "step": 0.1, "default": 1.0},
    }


# ---------- entry point ----------
def main():
    multiprocessing.freeze_support()

    global MASTER_RPC_HOST, MASTER_RPC_PORT, MASTER_HTTP_URL, IS_WORKER_MODE

    args = sys.argv[1:]
    if "--help" in args or "-h" in args:
        print(__doc__)
        return

    if "--worker" in args:
        IS_WORKER_MODE = True
        if "--master-host" in args:
            MASTER_RPC_HOST = args[args.index("--master-host") + 1]
        if "--master-rpc-port" in args:
            MASTER_RPC_PORT = int(args[args.index("--master-rpc-port") + 1])
        if "--master-url" in args:
            MASTER_HTTP_URL = args[args.index("--master-url") + 1]
        else:
            MASTER_HTTP_URL = f"http://{MASTER_RPC_HOST}:{BACKEND_PORT}"

        logger.info(
            "Worker mode: RPC %s:%d, HTTP %s",
            MASTER_RPC_HOST,
            MASTER_RPC_PORT,
            MASTER_HTTP_URL,
        )

        env = _create_env()
        env.create_worker_runner(
            master_host=MASTER_RPC_HOST, master_port=MASTER_RPC_PORT
        )
        env.runner.greenlet.join()
        return

    if "--master" in args:
        os.environ["LOCUST_WORKERS"] = "0"
        logger.info("Master mode: no local workers")

    uvicorn.run(app, host=BACKEND_HOST, port=BACKEND_PORT)


if __name__ == "__main__":
    main()
