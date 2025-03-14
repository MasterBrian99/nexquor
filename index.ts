import { promises as fs } from "fs";
import path from "path";
import SSTable from "./lib/ss-table";
import RecoverLog from "./lib/recovery-log";

// Initialize SSTable and RecoverLog
const recoverLog = new RecoverLog(SSTable.getAvlTree());

// Helper function to parse incoming commands
function parseCommand(data: string): {
  command: string;
  key?: string;
  value?: string;
} {
  const parts = data.trim().split(" ");
  const command = parts[0].toLowerCase();
  const key = parts[1] || undefined;
  const value = parts[2] || undefined;

  return { command, key, value };
}

// TCP server setup
const server = Bun.listen({
  hostname: "0.0.0.0",
  port: 6379,
  socket: {
    open(socket) {
      console.log("Client connected");
      socket.write("Welcome to the LSM Tree Storage TCP server!\n");
      socket.write("Available commands:\n");
      socket.write("- INSERT <key> <value>\n");
      socket.write("- UPDATE <key> <value>\n");
      socket.write("- DELETE <key>\n");
      socket.write("- READ <key>\n");
      socket.write("- RECOVER\n");
    },
    async data(socket, data) {
      const input = data.toString().trim();
      console.log(`Received: ${input}`);

      const { command, key, value } = parseCommand(input);

      try {
        let response = "";

        switch (command) {
          case "insert":
            if (!key || !value) {
              response = "Error: INSERT requires a key and value.\n";
            } else {
              await SSTable.insert(key, value);
              response = "Value Inserted\n";
            }
            break;

          case "update":
            if (!key || !value) {
              response = "Error: UPDATE requires a key and value.\n";
            } else {
              await SSTable.update(key, value);
              response = "Value Updated\n";
            }
            break;

          case "delete":
            if (!key) {
              response = "Error: DELETE requires a key.\n";
            } else {
              await SSTable.delete(key);
              response = "Value Deleted\n";
            }
            break;

          case "read":
            if (!key) {
              response = "Error: READ requires a key.\n";
            } else {
              const result = await SSTable.readKey(key);
              response = result ? `Value Read: ${result}\n` : "Key not found\n";
            }
            break;

          case "recover":
            await recoverLog.recover("log.txt");
            await SSTable.recoverFileNameAndSSTableMap();
            response = "Recovered\n";
            break;

          default:
            response =
              "Unknown command. Please use one of: INSERT, UPDATE, DELETE, READ, RECOVER\n";
        }

        // Send the response back to the client
        socket.write(response);
      } catch (error) {
        console.error("Error processing command:", error);
        socket.write(`Error: ${error.message}\n`);
      }
    },
    close(socket) {
      console.log("Client disconnected");
    },
    error(socket, error) {
      console.error("Socket error:", error);
    },
  },
});

console.log(`TCP server listening on ${server.hostname}:${server.port}`);
