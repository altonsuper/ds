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

DS3 Studio is a lightweight, single-file Flask web UI that connects directly to the [duckduckgo-chat-cli](https://github.com/benoitpetit/duckduckgo-chat-cli) local API. It gives you a modern, streaming chat interface with workspace management — all running on your machine, no API keys needed.

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

### Prerequisites
- Python 3.12+
- [duckduckgo-chat-cli](https://github.com/benoitpetit/duckduckgo-chat-cli) running with `/api 8989`

### Install & Run



```bash
pip install flask requests markdown-it-py pygments
python ds3.py
