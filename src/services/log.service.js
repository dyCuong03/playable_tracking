const fs = require("fs");
const path = require("path");

const DEFAULT_LOG_DIR = path.join(__dirname, "../../logs");
const TRACKING_LOG_FILE = "pixel-tracking.txt";

const readyDirs = new Set();

const getLogDir = () => process.env.PIXEL_LOG_DIR || DEFAULT_LOG_DIR;

const ensureLogDir = (dirPath = getLogDir()) => {
    if (readyDirs.has(dirPath)) {
        return;
    }

    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true });
    }

    readyDirs.add(dirPath);
};

const withTimestamp = (event) => ({
    ts: new Date().toISOString(),
    ...(event || {}),
});

const formatLocalDate = (date = new Date()) => {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");

    return `${year}-${month}-${day}`;
};

const appendPayload = (filePath, payload) => {
    try {
        ensureLogDir(path.dirname(filePath));
        fs.appendFileSync(filePath, payload, "utf8");
    } catch (error) {
        console.error("Failed to persist tracking log", error);
    }
};

exports.write = (event, silent = false) => {
    const entry = withTimestamp(event);
    const payload = JSON.stringify(entry);

    if (!silent) {
        console.log(payload);
    }

    appendPayload(path.join(getLogDir(), TRACKING_LOG_FILE), `${payload}\n`);
};

exports.writeDaily = (name, event, silent = true) => {
    const entry = withTimestamp(event);
    const payload = JSON.stringify(entry);

    if (!silent) {
        console.log(payload);
    }

    appendPayload(
        path.join(getLogDir(), name, `${formatLocalDate(new Date(entry.ts))}.ndjson`),
        `${payload}\n`
    );
};

exports.writeDailyBatch = (name, events, silent = true) => {
    if (!Array.isArray(events) || events.length === 0) {
        return;
    }

    const entries = events.map((event) => withTimestamp(event));

    if (!silent) {
        entries.forEach((entry) => console.log(JSON.stringify(entry)));
    }

    const batchesByDay = entries.reduce((acc, entry) => {
        const day = formatLocalDate(new Date(entry.ts));

        if (!acc.has(day)) {
            acc.set(day, []);
        }

        acc.get(day).push(entry);
        return acc;
    }, new Map());

    for (const [day, dayEntries] of batchesByDay.entries()) {
        const payload = dayEntries.map((entry) => JSON.stringify(entry)).join("\n") + "\n";

        appendPayload(
            path.join(getLogDir(), name, `${day}.ndjson`),
            payload
        );
    }
};
