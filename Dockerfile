# Use official Node.js image
FROM node:20-alpine

# Create working directory inside container
WORKDIR /src

# Copy package files first (for caching)
COPY package*.json ./

# Install only production dependencies
RUN npm install --production

# Copy project source files
COPY src ./src

# Expose server port
EXPOSE 3000

# Run server on start
CMD [ "node", "src/server.js" ]
