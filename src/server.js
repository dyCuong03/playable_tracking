const app = require("./app");
const { PORT } = require("./config");

app.listen(PORT, () => {
    console.log(`🚀 Pixel server running on port ${PORT}`);
});
