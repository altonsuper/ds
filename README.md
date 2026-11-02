<p align="center">
  <img src="images/duckai-1793615256378047600.jpeg" alt="DS3 Studio Banner" width="100%">
</p>

# 🦆 DS3 Studio

**A fast, local web UI for the DuckDuckGo Chat CLI.**

<p align="center">
  <img src="https://img.shields.io/badge/100%25_Local-Privacy_First-green" alt="Local">
  <img src="https://img.shields.io/badge/Open_Source-MIT-blue" alt="Open Source">
  <img src="https://img.shields.io/badge/Powered_by-duckduckgo--chat--cli-orange" alt="CLI">
</p>

---

## What is DS3?

DS3 Studio is a lightweight, single-file Flask web UI that connects directly to the [duckduckgo-chat-cli](https://github.com/benoitpetit/duckduckgo-chat-cli) local API. It gives you a modern, streaming chat interface with workspace management — all running on your machine. Get your own duck there ! 

## ✨ Features

- ⚡ **Real-time SSE streaming** replies (word-by-word)
- 🌓 **Light/Dark theme** toggle
- 📁 **Workspace file tree** with preview
- 💾 **Session persistence** (history survives restarts)
- 🎨 **Pygments syntax highlighting** for code blocks
- 🔍 **Built-in web search** (DuckDuckGo, set able on CLI)
- 📎 **Drag-and-drop file attachments**
- 📊 **Context budget gauge** (see prompt usage live)
- 🔄 **Clear & New Session** with upstream CLI reset

## 🚀 Quick Start

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12+-blue?logo=python&logoColor=white" alt="Python 3.12+">
  <img src="https://img.shields.io/badge/License-MIT-green" alt="License">
  <img src="https://img.shields.io/badge/UI-Flask_+_Vanilla_JS-orange" alt="UI Stack">
  <img src="https://img.shields.io/badge/Status-Production_Ready-brightgreen" alt="Status">
</p>

---

## 🚀 Quick Start

Get DS3 Studio running on your local machine in under 2 minutes.

### 📋 Prerequisites
- **Python 3.12 or newer**  
  Check your version by typing: `python --version`  
  *Don't have it?* [Download Python here](https://www.python.org/downloads/) *(⚠️ Make sure to check **"Add Python to PATH"** during installation!)*
- **DuckDuckGo Chat CLI** running locally in the background with the API enabled (`/api 8989`)

---

### 📥 Step 1: Download DS3
Get the latest release files directly from GitHub:  
👉 **[Download DS3 Release Here](https://github.com/altonsuper/ds/releases/tag/DS)**  
*(Extract the downloaded `.zip` file to a folder on your computer, e.g., `C:\ds3`)* 
👉 Better be side2 side with the duck cli folder `C:\Users\[yourname]\Documents\duckchat\`

### ⚙️ Step 2: Install Dependencies
Open your terminal (Command Prompt or PowerShell), navigate to the extracted folder, and install the required packages. 

**Option A: Standard Python (Recommended for beginners)**
```bash
pip install -r requirements.txt
```
```bash
if have uv :
uv pip install --system -r requirements.txt
(This automatically installs flask, requests, markdown-it-py, and pygments.)
```
🎯 Step 3: Run It!
Ensure your Duck CLI is running, then start DS3 in your terminal:
```bash
python ds3.py
```
You will see a success message like this:

  DS3 Studio  ->  http://127.0.0.1:7862
  Duck CLI    ->  http://127.0.0.1:8989
  Workspace   ->  C:\...\ds3\workspace
  Data        ->  C:\...\ds3\data

🎉 You're done! Open your web browser and go to:
👉 http://127.0.0.1:7862


ps Get your DUCK here : 
[https://github.com/benoitpetit/duckduckgo-chat-cli](https://github.com/benoitpetit/duckduckgo-chat-cli/releases)
get app --- run it --- type /api 8989 /dashboard on try mess inside the conf and run ds3.py

=====================================
⚡ THIS VERSION WAS HOPEFULLY UNIVERSALL !! ⚡
🙏🙏🙏🙏🙏🙏❤️❤️❤️❤️❤️❤️🙏🙏🙏🙏🙏🙏
=====================================
