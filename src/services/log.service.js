const fs = require("fs");
const path = require("path");

const LOG_DIR = path.join(__dirname, "../../logs");
const LOG_FILE = path.join(LOG_DIR, "pixel-tracking.txt");

let logDirReady = false;

const ensureLogDir = () => {
    if (logDirReady) {
        return;
    }

    if (!fs.existsSync(LOG_DIR)) {
        fs.mkdirSync(LOG_DIR, { recursive: true });
    }

    logDirReady = true;
};

exports.write = (event) => {
    const payload = JSON.stringify(event);
    console.log(payload);

    try {
        ensureLogDir();
        fs.appendFile(LOG_FILE, `${payload}\n`, (error) => {
            if (error) {
                console.error("Failed to append tracking log", error);
            }
        });
    } catch (error) {
        console.error("Failed to persist tracking log", error);
    }
};
