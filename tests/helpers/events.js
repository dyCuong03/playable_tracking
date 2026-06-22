"use strict";

// Builders for valid /p.gif query payloads, one per event stage. These mirror the
// validation contract in pixel.controller.validateEvent so generated traffic is accepted.

const PLATFORMS = ["Windows", "Android", "IOS"];

const startEvent = (sid, overrides = {}) => ({
    sid,
    e: "start",
    event_time: "2026-06-20T10:00:00.000Z",
    playableId: overrides.playableId || "playable-A",
    pid: overrides.pid || "com.archer.game",
    env: overrides.env || "test",
    event_params: JSON.stringify({
        network: overrides.network || "meta",
        platform: overrides.platform || PLATFORMS[0],
    }),
    ...stripParamKeys(overrides),
});

const interactionEvent = (sid, name, overrides = {}) => ({
    sid,
    e: overrides.event || "interaction",
    event_time: "2026-06-20T10:00:01.000Z",
    playableId: overrides.playableId || "playable-A",
    pid: overrides.pid || "com.archer.game",
    env: overrides.env || "test",
    event_params: JSON.stringify({ name, stage: overrides.stage || name }),
    ...stripParamKeys(overrides),
});

const storeTriggerEvent = (sid, name, overrides = {}) =>
    interactionEvent(sid, name, { ...overrides, event: "store_trigger" });

const endEvent = (sid, interactCount, overrides = {}) => ({
    sid,
    e: "end",
    event_time: "2026-06-20T10:00:09.000Z",
    playableId: overrides.playableId || "playable-A",
    pid: overrides.pid || "com.archer.game",
    env: overrides.env || "test",
    event_params: JSON.stringify({ interact_count: interactCount }),
    ...stripParamKeys(overrides),
});

const stripParamKeys = (overrides) => {
    const copy = { ...overrides };
    delete copy.playableId;
    delete copy.pid;
    delete copy.env;
    delete copy.network;
    delete copy.platform;
    delete copy.stage;
    delete copy.event;
    return copy;
};

module.exports = { startEvent, interactionEvent, storeTriggerEvent, endEvent, PLATFORMS };
