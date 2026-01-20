import amqp from "amqplib";

async function main() {
    console.log("Starting Peril server...");
    const connectionString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(connectionString);
    console.log("Connected to AMQP server.");
    process.on("SIGINT", () => {
        console.log("Shutting down...");
        conn.close();
    });
}

main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
});
