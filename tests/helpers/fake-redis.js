"use strict";

// In-memory fake of the subset of the `redis` client surface that
// src/services/redis-queue.service.js uses: sendCommand(argsArray) and
// MULTI().addCommand(args)/execAsPipeline(). It implements a real (enough) Redis
// Stream + consumer-group + key/value store so the ACTUAL production modules run
// unmodified against it — no behaviour is stubbed out, only the network is faked.
//
// It also records counters (xadd per stream, dedupe skips, consumed, acks, deletes)
// so tests can prove zero loss at the data level instead of trusting log strings,
// and supports fault injection (setFault) to simulate a Redis outage / restart.

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const createFakeRedis = () => {
    const streams = new Map(); // name -> { entries: [{id, seq, fields, deleted}], groups: Map }
    const kv = new Map(); // key -> { value, expireAt }
    let globalSeq = 0;

    const counters = {
        xaddByStream: {}, // streamName -> count of successful XADDs
        dedupeSkip: 0, // SET NX that returned null (duplicate)
        dedupeOk: 0, // SET NX that returned OK
        consumed: 0, // entries delivered via XREADGROUP + XAUTOCLAIM
        consumedByRead: 0,
        consumedByReclaim: 0,
        acked: 0,
        deleted: 0,
        commands: 0,
    };

    // fault.only: null => every command (and connect) fails; otherwise only the listed
    // uppercase command names fail (connect keeps working). Lets tests simulate a total
    // outage OR a targeted mid-pipeline XADD failure.
    const fault = { active: false, only: null };

    const faultHits = (command) =>
        fault.active && (!fault.only || fault.only.includes(String(command).toUpperCase()));

    const getStream = (name, create = false) => {
        if (!streams.has(name) && create) {
            streams.set(name, { entries: [], groups: new Map() });
        }
        return streams.get(name);
    };

    const now = () => Date.now();

    const isExpired = (record) => record.expireAt && record.expireAt <= now();

    const buildEntriesResponse = (entries) =>
        entries.map((entry) => [entry.id, flattenFields(entry.fields)]);

    const flattenFields = (fields) => {
        const flat = [];
        for (const [key, value] of Object.entries(fields)) {
            flat.push(key, value);
        }
        return flat;
    };

    const cmdSet = (args) => {
        // SET key value [NX] [EX ttl]
        const key = args[1];
        const value = args[2];
        const rest = args.slice(3).map((token) => String(token).toUpperCase());
        const nx = rest.includes("NX");
        const exIndex = rest.indexOf("EX");
        const ttlSeconds = exIndex >= 0 ? Number(args[3 + exIndex + 1]) : null;

        const existing = kv.get(key);
        if (existing && isExpired(existing)) {
            kv.delete(key);
        }

        if (nx && kv.has(key)) {
            counters.dedupeSkip += 1;
            return null;
        }

        kv.set(key, {
            value,
            expireAt: ttlSeconds ? now() + ttlSeconds * 1000 : null,
        });
        if (nx) {
            counters.dedupeOk += 1;
        }
        return "OK";
    };

    const liveRecord = (key) => {
        const record = kv.get(key);
        if (!record) {
            return null;
        }
        if (isExpired(record)) {
            kv.delete(key);
            return null;
        }
        return record;
    };

    const cmdGet = (args) => {
        const record = liveRecord(args[1]);
        return record ? record.value : null;
    };

    const cmdExists = (args) => args.slice(1).reduce((count, key) => count + (liveRecord(key) ? 1 : 0), 0);

    const cmdDel = (args) => args.slice(1).reduce((count, key) => count + (kv.delete(key) ? 1 : 0), 0);

    const cmdTtl = (args) => {
        const record = liveRecord(args[1]);
        if (!record) {
            return -2;
        }
        if (!record.expireAt) {
            return -1;
        }
        return Math.max(0, Math.ceil((record.expireAt - now()) / 1000));
    };

    const globToRegExp = (pattern) => {
        const escaped = String(pattern).replace(/[.+^${}()|[\]\\]/g, "\\$&");
        return new RegExp(`^${escaped.replace(/\*/g, ".*").replace(/\?/g, ".")}$`);
    };

    // SCAN cursor [MATCH pattern] [COUNT n] [TYPE t] -> [nextCursor, [keys...]].
    // We return everything matching in one pass (cursor always "0").
    const cmdScan = (args) => {
        const matchIndex = args.indexOf("MATCH");
        const pattern = matchIndex >= 0 ? args[matchIndex + 1] : "*";
        const matcher = globToRegExp(pattern);
        const keys = [];
        for (const key of kv.keys()) {
            if (matcher.test(key) && liveRecord(key)) {
                keys.push(key);
            }
        }
        return ["0", keys];
    };

    const cmdKeys = (args) => cmdScan([null, "0", "MATCH", args[1]])[1];

    const cmdXadd = (args) => {
        // XADD stream MAXLEN ~ N * payload <json>  (we ignore MAXLEN trimming)
        const name = args[1];
        const stream = getStream(name, true);
        const starIndex = args.indexOf("*");
        const fieldTokens = args.slice(starIndex + 1);
        const fields = {};
        for (let i = 0; i < fieldTokens.length; i += 2) {
            fields[fieldTokens[i]] = fieldTokens[i + 1];
        }

        globalSeq += 1;
        const id = `${globalSeq}-0`;
        stream.entries.push({ id, seq: globalSeq, fields, deleted: false });
        counters.xaddByStream[name] = (counters.xaddByStream[name] || 0) + 1;
        return id;
    };

    const cmdXgroupCreate = (args) => {
        // XGROUP CREATE stream group id MKSTREAM
        const name = args[2];
        const groupName = args[3];
        const stream = getStream(name, true);

        if (stream.groups.has(groupName)) {
            const error = new Error("BUSYGROUP Consumer Group name already exists");
            throw error;
        }

        stream.groups.set(groupName, { pel: new Map(), lastSeq: 0 });
        return "OK";
    };

    const cmdXreadgroup = async (args) => {
        // XREADGROUP GROUP group consumer COUNT n BLOCK ms STREAMS stream >
        const groupName = args[2];
        const consumer = args[3];
        const countIndex = args.indexOf("COUNT");
        const count = countIndex >= 0 ? Number(args[countIndex + 1]) : 100;
        const blockIndex = args.indexOf("BLOCK");
        const blockMs = blockIndex >= 0 ? Number(args[blockIndex + 1]) : 0;
        const streamsIndex = args.indexOf("STREAMS");
        const name = args[streamsIndex + 1];

        const stream = getStream(name, true);
        const group = stream.groups.get(groupName);
        if (!group) {
            throw new Error("NOGROUP No such consumer group");
        }

        const fresh = stream.entries
            .filter((entry) => !entry.deleted && entry.seq > group.lastSeq)
            .slice(0, count);

        if (!fresh.length) {
            if (blockMs > 0) {
                await delay(Math.min(blockMs, 20));
            }
            return null;
        }

        for (const entry of fresh) {
            group.lastSeq = Math.max(group.lastSeq, entry.seq);
            group.pel.set(entry.id, { consumer, idleSince: now(), seq: entry.seq });
        }

        counters.consumed += fresh.length;
        counters.consumedByRead += fresh.length;

        return [[name, buildEntriesResponse(fresh)]];
    };

    const cmdXautoclaim = (args) => {
        // XAUTOCLAIM stream group consumer min-idle start COUNT n
        const name = args[1];
        const groupName = args[2];
        const consumer = args[3];
        const minIdle = Number(args[4]);
        const countIndex = args.indexOf("COUNT");
        const count = countIndex >= 0 ? Number(args[countIndex + 1]) : 100;

        const stream = getStream(name, true);
        const group = stream.groups.get(groupName);
        if (!group) {
            return ["0-0", [], []];
        }

        const reclaimable = [...group.pel.entries()]
            .filter(([, meta]) => now() - meta.idleSince >= minIdle)
            .sort((a, b) => a[1].seq - b[1].seq)
            .slice(0, count);

        const claimedEntries = [];
        for (const [id, meta] of reclaimable) {
            const entry = stream.entries.find((candidate) => candidate.id === id && !candidate.deleted);
            if (!entry) {
                group.pel.delete(id);
                continue;
            }
            meta.consumer = consumer;
            meta.idleSince = now();
            claimedEntries.push(entry);
        }

        counters.consumed += claimedEntries.length;
        counters.consumedByReclaim += claimedEntries.length;

        return ["0-0", buildEntriesResponse(claimedEntries), []];
    };

    const cmdXack = (args) => {
        const name = args[1];
        const groupName = args[2];
        const ids = args.slice(3);
        const stream = getStream(name);
        const group = stream && stream.groups.get(groupName);
        if (!group) {
            return 0;
        }
        let count = 0;
        for (const id of ids) {
            if (group.pel.delete(id)) {
                count += 1;
            }
        }
        counters.acked += count;
        return count;
    };

    const cmdXdel = (args) => {
        const name = args[1];
        const ids = new Set(args.slice(2));
        const stream = getStream(name);
        if (!stream) {
            return 0;
        }
        let count = 0;
        for (const entry of stream.entries) {
            if (ids.has(entry.id) && !entry.deleted) {
                entry.deleted = true;
                count += 1;
            }
        }
        counters.deleted += count;
        return count;
    };

    const cmdXlen = (args) => {
        const stream = getStream(args[1]);
        if (!stream) {
            return 0;
        }
        return stream.entries.filter((entry) => !entry.deleted).length;
    };

    const cmdXpending = (args) => {
        const name = args[1];
        const groupName = args[2];
        const stream = getStream(name);
        const group = stream && stream.groups.get(groupName);
        if (!group || group.pel.size === 0) {
            return [0, null, null, []];
        }
        const ids = [...group.pel.keys()];
        const perConsumer = new Map();
        for (const meta of group.pel.values()) {
            perConsumer.set(meta.consumer, (perConsumer.get(meta.consumer) || 0) + 1);
        }
        return [
            group.pel.size,
            ids[0],
            ids[ids.length - 1],
            [...perConsumer.entries()].map(([consumer, c]) => [consumer, String(c)]),
        ];
    };

    const exec = async (args) => {
        counters.commands += 1;

        const command = String(args[0]).toUpperCase();

        if (faultHits(command)) {
            throw new Error(`FAKE_REDIS_DOWN simulated outage (${command})`);
        }

        switch (command) {
        case "SET":
            return cmdSet(args);
        case "GET":
            return cmdGet(args);
        case "EXISTS":
            return cmdExists(args);
        case "DEL":
        case "UNLINK":
            return cmdDel(args);
        case "TTL":
            return cmdTtl(args);
        case "SCAN":
            return cmdScan(args);
        case "KEYS":
            return cmdKeys(args);
        case "XADD":
            return cmdXadd(args);
        case "XGROUP":
            return cmdXgroupCreate(args);
        case "XREADGROUP":
            return cmdXreadgroup(args);
        case "XAUTOCLAIM":
            return cmdXautoclaim(args);
        case "XACK":
            return cmdXack(args);
        case "XDEL":
            return cmdXdel(args);
        case "XLEN":
            return cmdXlen(args);
        case "XPENDING":
            return cmdXpending(args);
        default:
            return "OK";
        }
    };

    const createClient = () => {
        const client = {
            isOpen: false,
            on() {},
            removeAllListeners() {},
            disconnect() {
                this.isOpen = false;
            },
            async connect() {
                if (faultHits("CONNECT")) {
                    throw new Error("FAKE_REDIS_DOWN connect refused");
                }
                this.isOpen = true;
                return this;
            },
            sendCommand(args) {
                return exec(args);
            },
            // High-level convenience surface (redis v4 client) routed through exec, so the
            // fake works whether the backend uses sendCommand([...]) or these helpers.
            async get(key) {
                return exec(["GET", key]);
            },
            async set(key, value, options = {}) {
                const args = ["SET", key, value];
                if (options && options.EX) {
                    args.push("EX", options.EX);
                }
                if (options && options.NX) {
                    args.push("NX");
                }
                return exec(args);
            },
            async ttl(key) {
                return exec(["TTL", key]);
            },
            async exists(key) {
                return exec(["EXISTS", key]);
            },
            async del(key) {
                return exec(["DEL", key]);
            },
            async keys(pattern) {
                return exec(["KEYS", pattern]);
            },
            async *scanIterator(options = {}) {
                const match = options && options.MATCH ? options.MATCH : "*";
                const [, keys] = await exec(["SCAN", "0", "MATCH", match]);
                for (const key of keys) {
                    yield key;
                }
            },
            MULTI() {
                const queued = [];
                return {
                    addCommand(args) {
                        queued.push(args);
                        return this;
                    },
                    async execAsPipeline() {
                        const results = [];
                        for (const args of queued) {
                            results.push(await exec(args));
                        }
                        return results;
                    },
                };
            },
        };
        return client;
    };

    return {
        module: { createClient },
        state: {
            streams,
            kv,
            counters,
            setFault(active, only = null) {
                fault.active = Boolean(active);
                fault.only = only ? only.map((command) => String(command).toUpperCase()) : null;
            },
            isFaulty() {
                return fault.active;
            },
            streamLength(name) {
                return cmdXlen([null, name]);
            },
            pendingCount(name, groupName) {
                const summary = cmdXpending([null, name, groupName]);
                return summary[0];
            },
            xaddCount(name) {
                return counters.xaddByStream[name] || 0;
            },
            // Direct key access for seeding/inspecting heartbeats in tests.
            setKey(key, value, ttlSeconds = null) {
                kv.set(key, {
                    value: typeof value === "string" ? value : JSON.stringify(value),
                    expireAt: ttlSeconds ? now() + ttlSeconds * 1000 : null,
                });
            },
            getKey(key) {
                const record = liveRecord(key);
                return record ? record.value : null;
            },
            scanKeys(pattern) {
                return cmdScan([null, "0", "MATCH", pattern])[1];
            },
            deleteKey(key) {
                return kv.delete(key);
            },
        },
    };
};

module.exports = { createFakeRedis };
