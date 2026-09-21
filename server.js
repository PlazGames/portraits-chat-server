const WebSocket = require("ws");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;

const users = {
    Agam: process.env.PASSWORD_AGAM,
    Satvik: process.env.PASSWORD_SATVIK,
    Darsh: process.env.PASSWORD_DARSH,
    Tejas: process.env.PASSWORD_TEJAS,
    Devansh: process.env.PASSWORD_DEVANSH
};

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
});

const server = new WebSocket.Server({
    host: "0.0.0.0",
    port: PORT
});

const connectedUsers = new Map();

function send(socket, data) {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(data));
    }
}

function broadcast(data) {
    const message = JSON.stringify(data);

    for (const socket of server.clients) {
        if (socket.readyState === WebSocket.OPEN) {
            socket.send(message);
        }
    }
}

function getOnlineUsers() {
    return Array.from(connectedUsers.keys());
}

function broadcastUserList() {
    broadcast({
        type: "users",
        users: getOnlineUsers()
    });
}

async function getMessageHistory() {
    const result = await pool.query(`
        SELECT
            username,
            text,
            EXTRACT(EPOCH FROM time) * 1000 AS time
        FROM messages
        ORDER BY time DESC
        LIMIT 200
    `);

    return result.rows.reverse().map(message => ({
        username: message.username,
        text: message.text,
        time: Number(message.time)
    }));
}

async function saveMessage(message) {
    const result = await pool.query(
        `
        INSERT INTO messages (username, text)
        VALUES ($1, $2)
        RETURNING
            username,
            text,
            EXTRACT(EPOCH FROM time) * 1000 AS time
        `,
        [message.username, message.text]
    );

    return {
        username: result.rows[0].username,
        text: result.rows[0].text,
        time: Number(result.rows[0].time)
    };
}

server.on("connection", socket => {
    let currentUsername = null;

    socket.on("message", async rawMessage => {
        let data;

        try {
            data = JSON.parse(rawMessage.toString());
        } catch {
            return;
        }

        if (data.type === "login") {
            const username = String(data.username || "");
            const password = String(data.password || "");

            if (!Object.prototype.hasOwnProperty.call(users, username)) {
                send(socket, {
                    type: "loginResult",
                    success: false,
                    error: "User not found."
                });

                return;
            }

            if (users[username] !== password) {
                send(socket, {
                    type: "loginResult",
                    success: false,
                    error: "Incorrect password."
                });

                return;
            }

            if (connectedUsers.has(username)) {
                send(socket, {
                    type: "loginResult",
                    success: false,
                    error: "This user is already online."
                });

                return;
            }

            try {
                const history = await getMessageHistory();

                currentUsername = username;

                connectedUsers.set(
                    username,
                    socket
                );

                send(socket, {
                    type: "loginResult",
                    success: true
                });

                send(socket, {
                    type: "history",
                    messages: history
                });

                broadcastUserList();

                broadcast({
                    type: "system",
                    text: username + " joined."
                });
            } catch {
                send(socket, {
                    type: "loginResult",
                    success: false,
                    error: "Could not load chat history."
                });
            }

            return;
        }

        if (data.type === "message") {
            if (!currentUsername) {
                return;
            }

            const text = String(data.text || "").trim();

            if (!text || text.length > 500) {
                return;
            }

            const message = {
                username: currentUsername,
                text: text
            };

            try {
                const savedMessage = await saveMessage(message);

                broadcast({
                    type: "message",
                    message: savedMessage
                });
            } catch {
                send(socket, {
                    type: "messageError",
                    error: "Message could not be saved."
                });
            }
        }
    });

    socket.on("close", () => {
        if (!currentUsername) {
            return;
        }

        connectedUsers.delete(currentUsername);

        broadcast({
            type: "system",
            text: currentUsername + " left."
        });

        broadcastUserList();
    });

    socket.on("error", () => {});
});

server.on("listening", () => {
    console.log(
        "Portraits Chat server running on port " + PORT
    );
});