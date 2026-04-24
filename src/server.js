const app = require("./app");
const { PORT } = require("./config");
const { initBigQueryQueue } = require("./services/bigquery-queue.service");

initBigQueryQueue();

app.listen(PORT, () => {
    console.log(`Pixel server running on port ${PORT}`);
});
