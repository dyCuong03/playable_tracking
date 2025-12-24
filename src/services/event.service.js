exports.buildEvent = (req) => ({
    time: new Date().toISOString(),
    event: req.query.e || "",
    pid: req.query.pid || "",
    sid: req.query.sid || "",
    ip: req.ip,
    ua: req.get("user-agent") || "",
    ref: req.get("referer") || "",
});
