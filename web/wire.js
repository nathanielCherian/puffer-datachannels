'use strict';

// The datagram format of the "media" DataChannel. The authoritative
// description is internal/wire/wire.go; everything is big-endian.
const Wire = {
  DATA: 1, PING: 2, PONG: 3, FEEDBACK: 16, NACK: 17, KEYREQ: 18,
  FLAG_KEY: 1, FLAG_RTX: 2, FLAG_AUDIO: 4,
  VIDEO: 0, AUDIO: 1,
  DATA_HEADER: 30,
  WHOLE_FRAME: 0xFFFF,
  MAX_FEEDBACK_ACKS: 90, // keeps a feedback message inside one packet
  MAX_NACK_ITEMS: 180,

  // DATA: server times are converted from Unix µs to ms.
  parseData(buf) {
    const d = new DataView(buf);
    const flags = d.getUint8(1);
    return {
      key: (flags & Wire.FLAG_KEY) !== 0,
      rtx: (flags & Wire.FLAG_RTX) !== 0,
      audio: (flags & Wire.FLAG_AUDIO) !== 0,
      seq: d.getUint32(2),
      sendMs: Number(d.getBigInt64(6)) / 1000,
      frameId: d.getUint32(14),
      fragIdx: d.getUint16(18),
      fragCount: d.getUint16(20),
      captureMs: Number(d.getBigInt64(22)) / 1000,
      payload: new Uint8Array(buf, Wire.DATA_HEADER),
      size: buf.byteLength,
    };
  },

  parsePong(buf) {
    const d = new DataView(buf);
    return { id: d.getUint32(1), t1: d.getFloat64(5), t2: Number(d.getBigInt64(13)) / 1000, t3: Number(d.getBigInt64(21)) / 1000 };
  },

  ping(id, t1) {
    const d = new DataView(new ArrayBuffer(13));
    d.setUint8(0, Wire.PING);
    d.setUint32(1, id);
    d.setFloat64(5, t1);
    return d.buffer;
  },

  // acks: [[seq, recvMs], ...]
  feedback(fbSeq, sendMs, acks) {
    const d = new DataView(new ArrayBuffer(15 + 12 * acks.length));
    d.setUint8(0, Wire.FEEDBACK);
    d.setUint32(1, fbSeq);
    d.setFloat64(5, sendMs);
    d.setUint16(13, acks.length);
    acks.forEach(([seq, recvMs], i) => {
      d.setUint32(15 + 12 * i, seq);
      d.setFloat64(19 + 12 * i, recvMs);
    });
    return d.buffer;
  },

  // stream: Wire.VIDEO or Wire.AUDIO; items: [[frameId, fragIdx], ...]
  nack(stream, items) {
    const d = new DataView(new ArrayBuffer(4 + 6 * items.length));
    d.setUint8(0, Wire.NACK);
    d.setUint8(1, stream);
    d.setUint16(2, items.length);
    items.forEach(([frameId, fragIdx], i) => {
      d.setUint32(4 + 6 * i, frameId);
      d.setUint16(8 + 6 * i, fragIdx);
    });
    return d.buffer;
  },

  keyRequest(frameId) {
    const d = new DataView(new ArrayBuffer(5));
    d.setUint8(0, Wire.KEYREQ);
    d.setUint32(1, frameId >>> 0);
    return d.buffer;
  },
};
