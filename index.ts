
const server = Bun.listen({
    hostname: '0.0.0.0', 
    port: 6379,        
    socket: {
      open(socket) {
        console.log('Client connected');
        socket.write('Hello from Bun TCP server!\n');
      },
      data(socket, data) {
        console.log('Received:', data.toString());
        socket.write(`You said: ${data}`);
      },
      close(socket) {
        console.log('Client disconnected');
      },
      error(socket, error) {
        console.error('Error:', error);
      },
    },
  });
  
  console.log(`TCP server listening on ${server.hostname}:${server.port}`);