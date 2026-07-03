#!/usr/bin/env node
/**
 * octo-bridge.js — WuKongIM 协议桥接（多 bot）
 *
 * 支持多个 bot_token 同时连接，每条消息携带 bot_id 标识来源。
 */
const WebSocket = require('ws');
const { EventEmitter } = require('events');
const { generateKeyPair, sharedKey } = require('curve25519-js');
const { Md5 } = require('md5-typescript');

const PacketType = { CONNECT:1, CONNACK:2, SEND:3, SENDACK:4, RECV:5, RECVACK:6, PING:7, PONG:8, DISCONNECT:9 };
const PROTO_VERSION = 4;

function generateDeviceID() {
  return 'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

function encodeVariableLength(len) {
  const ret = [];
  while (len > 0) { let digit = len % 0x80; len = Math.floor(len / 0x80); if (len > 0) digit |= 0x80; ret.push(digit); }
  return ret;
}

function parseSettingByte(v) {
  return { receiptEnabled: ((v >> 7) & 0x01) > 0, topic: ((v >> 3) & 0x01) > 0, streamOn: ((v >> 1) & 0x01) > 0 };
}

class Encoder {
  constructor() { this.w = []; }
  writeByte(b) { this.w.push(b & 0xff); }
  writeBytes(b) { this.w.push(...b); }
  writeInt16(b) { this.w.push((b >> 8) & 0xff, b & 0xff); }
  writeInt32(b) { this.w.push((b >> 24) & 0xff, (b >> 16) & 0xff, (b >> 8) & 0xff, b & 0xff); }
  writeInt64(n) { this.writeInt32(Number(n >> 32n)); this.writeInt32(Number(n & 0xffffffffn)); }
  writeString(s) {
    if (s && s.length > 0) { const arr = Buffer.from(s, 'utf8'); this.writeInt16(arr.length); this.w.push(...arr); }
    else { this.writeInt16(0); }
  }
  toUint8Array() { return Buffer.from(this.w); }
}

class Decoder {
  constructor(data) { this.data = Buffer.from(data); this.offset = 0; }
  readByte() { return this.data[this.offset++]; }
  readInt16() { const v = (this.data[this.offset] << 8) | this.data[this.offset + 1]; this.offset += 2; return v; }
  readInt32() { const v = (this.data[this.offset] << 24) | (this.data[this.offset + 1] << 16) | (this.data[this.offset + 2] << 8) | this.data[this.offset + 3]; this.offset += 4; return v >>> 0; }
  readInt64String() { let n = BigInt(0); for (let i = 0; i < 8; i++) n = (n << 8n) | BigInt(this.data[this.offset + i]); this.offset += 8; return n.toString(); }
  readString() { const len = this.readInt16(); if (len <= 0) return ''; const s = this.data.slice(this.offset, this.offset + len); this.offset += len; return s.toString('utf8'); }
  readRemaining() { const d = this.data.slice(this.offset); this.offset = this.data.length; return d; }
  readVariableLength() { let m = 0, r = 0; while (m < 27) { const b = this.readByte(); r = r | ((b & 127) << m); if ((b & 128) === 0) break; m += 7; } return r; }
}

function encodeConnectPacket(opts) {
  const body = new Encoder();
  body.writeByte(opts.version); body.writeByte(opts.deviceFlag);
  body.writeString(opts.deviceID); body.writeString(opts.uid);
  body.writeString(opts.token); body.writeInt64(BigInt(opts.clientTimestamp));
  body.writeString(opts.clientKey);
  const bb = Array.from(body.toUint8Array());
  const frame = new Encoder();
  frame.writeByte((PacketType.CONNECT << 4) | 0);
  frame.writeBytes(encodeVariableLength(bb.length));
  frame.writeBytes(bb);
  return frame.toUint8Array();
}

function encodePingPacket() { return Buffer.from([(PacketType.PING << 4) | 0]); }

function encodeRecvackPacket(messageID, messageSeq) {
  const body = new Encoder();
  body.writeInt64(BigInt(messageID)); body.writeInt32(messageSeq);
  const bb = Array.from(body.toUint8Array());
  const frame = new Encoder();
  frame.writeByte((PacketType.RECVACK << 4) | 0);
  frame.writeBytes(encodeVariableLength(bb.length));
  frame.writeBytes(bb);
  return frame.toUint8Array();
}

function aesDecrypt(data, aesKey, aesIV) {
  const { createDecipheriv } = require('crypto');
  const payloadBase64 = Buffer.from(data).toString('utf8').trim();
  const raw = Buffer.from(payloadBase64, 'base64');
  if (raw.length === 0 || raw.length % 16 !== 0) {
    console.warn(`[bridge] decrypt payload: base64Len=${payloadBase64.length} cipherLen=${raw.length} cipherMod16=${raw.length % 16} keyLen=${aesKey.length} ivLen=${aesIV.length}`);
  }
  const decipher = createDecipheriv('aes-128-cbc', Buffer.from(aesKey, 'utf8'), Buffer.from(aesIV, 'utf8'));
  return Buffer.concat([decipher.update(raw), decipher.final()]);
}

class WKSocket extends EventEmitter {
  constructor(opts) {
    super();
    this.wsUrl = opts.wsUrl; this.uid = opts.uid; this.token = opts.token;
    this.apiUrl = opts.apiUrl; this.botToken = opts.botToken;
    this.botId = opts.botId || '';
    this.ws = null; this.connected = false; this.needReconnect = true;
    this.heartTimer = null; this.aesKey = ''; this.aesIV = '';
    this.dhPrivateKey = null; this.serverVersion = 0;
    this.tempBuffer = []; this.reconnectAttempts = 0;
  }

  connect() { this.needReconnect = true; this.doConnect(); }

  disconnect() {
    this.needReconnect = false; this.connected = false; this.stopHeart();
    if (this.ws) { try { this.ws.close(); } catch {} this.ws = null; }
  }

  doConnect() {
    if (this.ws) { try { this.ws.close(); } catch {} this.ws = null; }
    this.tempBuffer = [];
    const ws = new WebSocket(this.wsUrl);
    ws.binaryType = 'arraybuffer';
    this.ws = ws;

    ws.on('open', () => {
      if (this.ws !== ws) return;
      this.tempBuffer = [];
      const seed = Buffer.from(generateDeviceID(), 'utf8');
      const seedBytes = new Uint8Array(32);
      for (let i = 0; i < 32 && i < seed.length; i++) seedBytes[i] = seed[i];
      const keyPair = generateKeyPair(seedBytes);
      this.dhPrivateKey = keyPair.private;
      const pubKey = Buffer.from(keyPair.public).toString('base64');
      const deviceID = generateDeviceID() + 'W';
      const packet = encodeConnectPacket({
        version: PROTO_VERSION, deviceFlag: 0, deviceID,
        uid: this.uid, token: this.token,
        clientTimestamp: Math.floor(Date.now() / 1000),
        clientKey: pubKey,
      });
      ws.send(packet);
      console.log(`[bridge] CONNECT sent (uid=${this.uid} botId=${this.botId})`);
    });

    ws.on('message', (data) => {
      if (this.ws !== ws) return;
      this.handleRawData(Buffer.from(data));
    });

    ws.on('close', () => {
      if (this.ws !== ws) return;
      if (this.connected) { this.connected = false; this.emit('disconnected'); }
      this.stopHeart();
      if (this.needReconnect) {
        const delay = Math.min(3000 * Math.pow(2, this.reconnectAttempts), 60000);
        console.log(`[bridge] Reconnecting botId=${this.botId} in ${delay}ms...`);
        setTimeout(() => { if (this.needReconnect) { this.reconnectAttempts++; this.doConnect(); } }, delay);
      }
    });

    ws.on('error', (err) => {
      if (this.ws !== ws) return;
      console.error(`[bridge] WS error (botId=${this.botId}): ${err.message}`);
    });
  }

  handleRawData(data) {
    this.tempBuffer.push(...Array.from(data));
    try {
      let lenBefore;
      do {
        lenBefore = this.tempBuffer.length;
        this.tempBuffer = this.unpackOne(this.tempBuffer);
      } while (lenBefore !== this.tempBuffer.length && this.tempBuffer.length >= 1);
    } catch (err) {
      console.error('[bridge] decode error:', err.message);
      this.tempBuffer = [];
      if (this.ws) { try { this.ws.close(); } catch {} }
    }
  }

  unpackOne(data) {
    if (data.length === 0) return data;
    const header = data[0], packetType = header >> 4;
    if (packetType === PacketType.PONG) return data.slice(1);
    if (packetType === PacketType.PING) return data.slice(1);

    let pos = 1, remLength = 0, multiplier = 1, hasMore = false, remLengthFull = true;
    do {
      if (pos > data.length - 1) { remLengthFull = false; break; }
      const digit = data[pos++];
      remLength += (digit & 127) * multiplier;
      multiplier *= 128;
      hasMore = (digit & 0x80) !== 0;
    } while (hasMore);
    if (!remLengthFull) return data;
    const totalLength = 1 + (pos - 1) + remLength;
    if (totalLength > data.length) return data;

    this.onPacket(new Uint8Array(data.slice(0, totalLength)));
    return data.slice(totalLength);
  }

  onPacket(data) {
    const firstByte = data[0], packetType = firstByte >> 4, hasServerVersion = (firstByte & 0x01) > 0;
    const dec = new Decoder(data);
    dec.readByte();
    if (packetType !== PacketType.PING && packetType !== PacketType.PONG) dec.readVariableLength();

    switch (packetType) {
      case PacketType.CONNACK: this.onConnack(dec, hasServerVersion); break;
      case PacketType.RECV: this.onRecv(dec); break;
      case PacketType.DISCONNECT: this.onDisconnect(dec); break;
      case PacketType.SENDACK: break;
    }
  }

  onConnack(dec, hasServerVersion) {
    if (hasServerVersion) this.serverVersion = dec.readByte();
    const _timeDiff = dec.readInt64String();
    const reasonCode = dec.readByte();
    const serverKey = dec.readString();
    const salt = dec.readString();
    if (this.serverVersion >= 4) { const _nodeId = dec.readInt64String(); }

    console.log(`[bridge] CONNACK: reasonCode=${reasonCode}, serverVersion=${this.serverVersion} botId=${this.botId}`);

    if (reasonCode === 1) {
      const serverPubKey = new Uint8Array(Buffer.from(serverKey, 'base64'));
      const secret = sharedKey(this.dhPrivateKey, serverPubKey);
      const secretBase64 = Buffer.from(secret).toString('base64');
      const aesKeyFull = Md5.init(secretBase64);
      this.aesKey = aesKeyFull.substring(0, 16);
      this.aesIV = salt && salt.length > 16 ? salt.substring(0, 16) : salt;
      this.connected = true; this.reconnectAttempts = 0; this.restartHeart();
      if (this.ws && this.ws.readyState === 1) this.ws.send(encodePingPacket());
      this.sendHttpHeartbeat();
      console.log(`[bridge] WuKongIM authenticated (botId=${this.botId})`);
      this.emit('connected');
    } else if (reasonCode === 0) {
      this.connected = false; this.needReconnect = false;
      this.emit('error', new Error('Kicked'));
    } else {
      this.connected = false; this.needReconnect = false;
      this.emit('error', new Error(`Connect failed: ${reasonCode}`));
    }
  }

  onRecv(dec) {
    const settingByte = dec.readByte();
    const setting = parseSettingByte(settingByte);
    const _msgKey = dec.readString();
    const fromUID = dec.readString();
    const channelID = dec.readString();
    const channelType = dec.readByte();
    if (this.serverVersion >= 3) { const _expire = dec.readInt32(); }
    const _clientMsgNo = dec.readString();
    const messageID = dec.readInt64String();
    const messageSeq = dec.readInt32();
    const timestamp = dec.readInt32();
    if (setting.topic) { const _topic = dec.readString(); }
    const encryptedPayload = dec.readRemaining();

    console.log(`[bridge] RECV: from=${fromUID} channel=${channelID || 'DM'} type=${channelType} seq=${messageSeq} botId=${this.botId}`);

    const recvack = encodeRecvackPacket(messageID, messageSeq);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(recvack);

    let payloadObj;
    try {
      const decryptedBytes = aesDecrypt(encryptedPayload, this.aesKey, this.aesIV);
      const payloadStr = decryptedBytes.toString('utf8');
      payloadObj = JSON.parse(payloadStr);
    } catch (err) {
      console.error(`[bridge] decrypt error (botId=${this.botId}):`, err.message);
      return;
    }

    this.emit('message', {
      bot_id: this.botId,
      message_id: messageID, message_seq: messageSeq,
      from_uid: fromUID, channel_id: channelID, channel_type: channelType, timestamp,
      payload: { type: payloadObj?.type ?? 0, content: payloadObj?.content, ...payloadObj },
    });
  }

  onDisconnect(dec) {
    const reasonCode = dec.readByte();
    this.connected = false; this.needReconnect = false; this.stopHeart();
    this.emit('error', new Error(`Kicked, reason=${reasonCode}`));
  }

  restartHeart() {
    this.stopHeart();
    this.heartTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(encodePingPacket());
      this.sendHttpHeartbeat();
    }, 30000);
  }

  stopHeart() { if (this.heartTimer) { clearInterval(this.heartTimer); this.heartTimer = null; } }

  sendHttpHeartbeat() {
    if (!this.apiUrl || !this.botToken) return;
    fetch(`${this.apiUrl}/v1/bot/heartbeat`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${this.botToken}`, 'Content-Type': 'application/json' },
      body: '{}',
    }).catch(() => {});
  }
}

async function registerBot(apiUrl, botToken) {
  const resp = await fetch(`${apiUrl}/v1/bot/register`, {
    method: 'POST', headers: { 'Authorization': `Bearer ${botToken}`, 'Content-Type': 'application/json' }, body: '{}',
  });
  if (!resp.ok) { throw new Error(`register failed: ${resp.status}`); }
  return resp.json();
}

async function main() {
  const args = process.argv.slice(2);
  const getArg = (name, def) => { const i = args.indexOf(`--${name}`); return i >= 0 && args[i + 1] ? args[i + 1] : def; };
  const apiUrl = getArg('api-url', 'https://im.deepminer.com.cn/api');
  const port = parseInt(getArg('port', '9876'));

  // 多 bot 配置：通过 --bots 传入 JSON 数组
  const botsJson = getArg('bots', '');
  if (!botsJson) { console.error('--bots required'); process.exit(1); }
  let botConfigs;
  try { botConfigs = JSON.parse(botsJson); } catch { console.error('--bots JSON parse error'); process.exit(1); }
  if (!Array.isArray(botConfigs) || botConfigs.length === 0) { console.error('No bots configured'); process.exit(1); }

  console.log(`[bridge] API: ${apiUrl}, port: ${port}, bots: ${botConfigs.length}`);

  // JSON WS 服务（Python 端连接）
  const wss = new WebSocket.Server({ port });
  console.log(`[bridge] JSON WS: ws://127.0.0.1:${port}`);
  let pythonWs = null;
  wss.on('connection', (ws) => {
    console.log('[bridge] Python connected');
    pythonWs = ws;
    ws.on('close', () => { pythonWs = null; });
  });

  // 为每个 bot 注册并建立 WS 连接
  const sockets = [];
  for (const cfg of botConfigs) {
    const botToken = cfg.bot_token;
    const botId = cfg.bot_id || botToken;
    try {
      const creds = await registerBot(apiUrl, botToken);
      console.log(`[bridge] Bot registered: robot_id=${creds.robot_id} botId=${botId}`);
      const socket = new WKSocket({ wsUrl: creds.ws_url, uid: creds.robot_id, token: creds.im_token, apiUrl, botToken, botId });
      socket.on('connected', () => console.log(`[bridge] WuKongIM authenticated (botId=${botId})`));
      socket.on('disconnected', () => console.log(`[bridge] WuKongIM disconnected (botId=${botId})`));
      socket.on('error', (err) => console.error(`[bridge] WuKongIM error (botId=${botId}):`, err.message));
      socket.on('message', (msg) => {
        console.log(`[bridge] MSG from=${msg.from_uid} ch=${msg.channel_id || 'DM'} type=${msg.channel_type} botId=${msg.bot_id}`);
        if (pythonWs && pythonWs.readyState === WebSocket.OPEN) pythonWs.send(JSON.stringify({ type: 'message', data: msg }));
      });
      socket.connect();
      sockets.push(socket);
    } catch (err) {
      console.error(`[bridge] Bot registration failed (botId=${botId}): ${err.message}`);
    }
  }

  console.log(`[bridge] Running with ${sockets.length}/${botConfigs.length} bots connected.`);
}

main().catch((err) => { console.error('[bridge] Fatal:', err); process.exit(1); });
