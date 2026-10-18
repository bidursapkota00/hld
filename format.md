# Docker & Docker Hub Complete Guide

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

![Docker and Docker Hub Complete Guide by Bidur Sapkota](docker-1200.webp "Docker and Docker Hub Complete Guide – Blog by Bidur Sapkota")

## Table of Contents

1. [Introducing Docker](#introducing-docker)
2. [Installation & Setup](#installation--setup)
3. [Docker Images](#docker-images)
4. [Docker Containers](#docker-containers)
5. [Dockerizing a FastAPI App](#dockerizing-a-fastapi-app)
6. [Docker Volumes](#docker-volumes)
7. [Docker Networking](#docker-networking)
8. [Docker Compose](#docker-compose)
9. [Docker Hub](#docker-hub)
10. [Multi-Stage Builds](#multi-stage-builds)
11. [Environment Variables & .env Files](#environment-variables--env-files)
12. [Docker Logs & Debugging](#docker-logs--debugging)
13. [Docker Prune & Cleanup](#docker-prune--cleanup)

---

## Introducing Docker

Docker is a platform that packages applications and their dependencies into lightweight, portable containers. Unlike virtual machines, containers share the host OS kernel, making them faster to start, smaller in size, and more efficient with resources. A container bundles your code, runtime, libraries, and system tools into a single unit that runs identically on any machine with Docker installed.

Docker lets you eliminate "works on my machine" problems, ship applications with consistent environments across development, testing, and production, isolate services from each other, scale applications quickly, and simplify CI/CD pipelines.

Building blocks of Docker are:

- **Image**: A read-only template with instructions for creating a container. Think of it as a snapshot of your application and its environment.
- **Container**: A running instance of an image. You can run many containers from the same image.
- **Dockerfile**: A text file with step-by-step instructions to build an image.
- **Docker Hub**: A cloud-based registry to store, share, and pull Docker images.
- **Volume**: Persistent storage that survives container restarts and removal.
- **Network**: A virtual network that lets containers communicate with each other.

---

## Installation & Setup

### Install Docker

**macOS / Windows**: Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/).

**Linux (Ubuntu/Debian)**:

```bash
sudo apt update
sudo apt install docker.io -y
sudo systemctl start docker
sudo systemctl enable docker

# Run docker without sudo
sudo usermod -aG docker $USER
# Log out and back in for group changes to take effect
```

`-y` automatically confirms the installation prompt. `systemctl start` starts the Docker daemon immediately. `systemctl enable` makes Docker start automatically on boot. `usermod -aG docker $USER` adds your user to the `docker` group so you can run Docker commands without `sudo`.

### Verify Installation

```bash
docker --version
docker run hello-world
```

`docker run hello-world` pulls and runs a test image to confirm Docker is working correctly.

---

## Docker Images

An image is a lightweight, standalone package that includes everything needed to run a piece of software, including code, runtime, libraries, and configuration.

### Pulling Images

```bash
docker pull nginx                     # Pull latest nginx image
docker pull python:3.14-slim          # Pull specific version
docker pull ubuntu:22.04              # Pull specific Ubuntu version
```

Without a tag (`:version`), Docker defaults to `:latest`. The tag after the colon specifies the exact version to pull. `-slim` variants are smaller images with minimal packages.

### Listing Images

```bash
docker images                         # List all local images
docker images -q                      # List only image IDs
```

`-q` (quiet) shows only the numeric IDs, useful for scripting.

### Removing Images

```bash
docker rmi python:3.14-slim          # Remove by name
docker rmi 3a4e5b6c7d8e              # Remove by image ID
docker rmi $(docker images -q)       # Remove all images
```

`rmi` stands for "remove image". The `$(...)` syntax runs the inner command and passes its output as arguments.

### Inspecting Images

```bash
docker inspect nginx                  # Detailed image metadata (JSON)
docker history nginx                  # Show layers and build steps
```

`inspect` outputs full JSON metadata including configuration, layers, and network settings. `history` shows each layer, the command that created it, and its size.

---

## Docker Containers

A container is a running instance of an image. You can create, start, stop, and remove containers independently.

### Running Containers

```bash
docker run nginx                      # Run in foreground (blocks terminal)
docker run -d nginx                   # Run in background (detached)
docker run -d --name web nginx        # Run with a custom name
docker run -d -p 8080:80 nginx        # Map host port 8080 to container port 80
docker run -it ubuntu bash            # Interactive terminal inside container
```

`-d` (detached) runs the container in the background and prints the container ID. `--name web` assigns a human-readable name instead of a random one. `-p 8080:80` maps port 8080 on your host machine to port 80 inside the container, so `localhost:8080` reaches the container's port 80. `-it` combines `-i` (interactive, keeps stdin open) and `-t` (allocates a pseudo-TTY), giving you a terminal inside the container.

### Listing Containers

```bash
docker ps                             # Show running containers
docker ps -a                          # Show all containers (including stopped)
docker ps -q                          # Show only container IDs
```

`-a` (all) includes containers that have exited or been stopped. `-q` (quiet) shows only the numeric IDs.

### Stopping & Starting Containers

```bash
docker stop web                       # Gracefully stop (sends SIGTERM)
docker start web                      # Start a stopped container
docker restart web                    # Restart a running container
docker kill web                       # Force stop (sends SIGKILL)
```

`stop` sends SIGTERM and waits (default 10 seconds) for the process to exit gracefully before sending SIGKILL. `kill` sends SIGKILL immediately without waiting.

### Executing Commands in a Running Container

```bash
docker exec -it web bash              # Open shell inside running container
docker exec web cat /etc/os-release   # Run a single command
```

`exec` runs a command inside an already-running container. `-it` gives an interactive terminal. Without `-it`, the command runs and outputs the result to your host terminal.

### Removing Containers

```bash
docker rm web                         # Remove a stopped container
docker rm -f web                      # Force remove (even if running)
docker rm $(docker ps -aq)            # Remove all stopped containers
```

`-f` (force) stops the container first and then removes it. `docker ps -aq` lists all container IDs including stopped ones.

### Copying Files

```bash
docker cp myfile.txt web:/app/        # Copy from host to container
docker cp web:/app/data.txt ./        # Copy from container to host
```

The first argument is the source and the second is the destination. The format `container_name:/path` refers to a path inside the container.

---

## Dockerizing a FastAPI App

This section builds a single project that all remaining sections will use. You will create the app, write a Dockerfile, and learn every Dockerfile instruction along the way.

### Create Virtual Environment

Since we are working locally before Dockerizing, create a venv to isolate dependencies:

```bash
python -m venv venv
source venv/bin/activate   # macOS/Linux
venv\Scripts\activate      # Windows
```

### FastAPI Setup

Install FastAPI with all standard dependencies:

```bash
pip install "fastapi[standard]"
```

`fastapi[standard]` includes FastAPI along with `uvicorn` (ASGI server), `pydantic` (data validation), and other production-ready dependencies.

### Create `main.py`

```python
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, Docker!"}
```

`@app.get("/")` defines a GET endpoint at the root path. `{item_id}` is a path parameter automatically parsed as an `int`. `q: str = None` is an optional query parameter.

### Run Locally (Without Docker)

```bash
fastapi dev main.py
```

This starts the development server at `http://127.0.0.1:8000` with auto-reload enabled. Interactive API docs are available at `http://127.0.0.1:8000/docs`.

### Create `requirements.txt`

```bash
pip freeze > requirements.txt
```

### Dockerfile Instructions Reference

A Dockerfile is a text file that contains instructions to build a Docker image, one instruction per line. Docker reads each instruction, executes it, and creates a new image layer.

| Instruction  | Purpose                                                   |
| ------------ | --------------------------------------------------------- |
| `FROM`       | Base image to build upon (must be first instruction)      |
| `WORKDIR`    | Set working directory inside the container                |
| `COPY`       | Copy files/directories from host to container             |
| `ADD`        | Like COPY but also handles URLs and tar extraction        |
| `RUN`        | Execute a command during the build (creates a new layer)  |
| `CMD`        | Default command to run when the container starts          |
| `ENTRYPOINT` | Like CMD but harder to override; sets the main executable |
| `EXPOSE`     | Document which port the container listens on              |
| `ENV`        | Set environment variables                                 |
| `ARG`        | Define build-time variables                               |
| `VOLUME`     | Create a mount point for persistent data                  |

### Create `Dockerfile`

```dockerfile
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["fastapi", "run", "main.py", "--port", "8000"]
```

`FROM python:3.14-slim` starts with a minimal Python 3.14 image. `WORKDIR /app` sets `/app` as the working directory for all subsequent instructions; it is created if it does not exist. `COPY requirements.txt .` copies only the requirements file first. `RUN` executes during the build; `--no-cache-dir` prevents pip from caching downloaded packages, reducing the image size. `COPY . .` then copies the rest of the source code. This two-step copy order means Docker caches the dependency installation layer. If only your source code changes, Docker reuses the cached `RUN pip install` layer, making rebuilds much faster. `EXPOSE 8000` documents that the app listens on port 8000 but does not actually publish the port, so you still need `-p`. `CMD` specifies the default command when a container starts; using the exec form `["fastapi", "run", ...]` is preferred over the shell form. `fastapi run` starts the production server using Uvicorn internally.

### CMD vs ENTRYPOINT

```dockerfile
# CMD can be overridden entirely by docker run arguments
CMD ["fastapi", "run", "main.py", "--port", "8000"]

# ENTRYPOINT stays fixed; docker run arguments are appended
ENTRYPOINT ["fastapi"]
CMD ["run", "main.py", "--port", "8000"]
```

With the combined form, `docker run myimage dev main.py` would run `fastapi dev main.py`. The entrypoint (`fastapi`) stays fixed and the CMD is replaced by the arguments you pass.

### Create `.dockerignore`

Like `.gitignore`, a `.dockerignore` file excludes files from the build context to reduce image size and avoid copying sensitive data. Before building the Docker image, create a `.dockerignore` file at the project root (same level as Dockerfile). This ensures Docker does NOT copy unnecessary files like your local virtual environment.

```text
# Virtual environment
venv/
.venv/

# Python cache
__pycache__/
*.pyc

# Git / environment files
.git/
.env

# IDE files
.vscode/
.idea/
```

### Build & Run

```bash
docker build -t fastapi-app .         # Build image, tag it "fastapi-app"
docker run -d -p 8000:8000 --name api fastapi-app
```

`-t fastapi-app` tags (names) the image for easy reference. `.` specifies the build context, meaning Docker looks for a Dockerfile in the current directory. `-p 8000:8000` maps host port 8000 to container port 8000. Visit `http://localhost:8000` and `http://localhost:8000/docs` to see your API.

---

## Docker Volumes

Containers are ephemeral, so data inside a container is lost when it is removed. Volumes provide persistent storage that exists independently of containers.

### Types of Mounts

| Type        | Description                                        |
| ----------- | -------------------------------------------------- |
| Volume      | Managed by Docker, stored in Docker's storage area |
| Bind Mount  | Maps a specific host directory into the container  |
| tmpfs Mount | Stored in host memory only, never written to disk  |

### Named Volumes

```bash
docker volume create app-data         # Create a named volume
docker volume ls                      # List all volumes
docker volume inspect app-data        # Show volume details

docker run -d -v app-data:/data --name api fastapi-app
```

`-v app-data:/data` mounts the volume `app-data` at `/data` inside the container. Data written to `/data` persists across container restarts and removal.

### Bind Mounts

```bash
docker run -d -v $(pwd):/app -p 8000:8000 --name api fastapi-app

# complete
docker run -d -v $(pwd):/app -v app-data:/data -p 8000:8000 --name api fastapi-app
```

On Windows, run these commands in PowerShell and use `${pwd}` instead of `$(pwd)`.

`$(pwd)` expands to the current working directory on the host. Bind mounts reflect changes immediately in both directions. Edit files on your host and the container sees them instantly. Useful for development.

To see bind mounts in action, start the container with the development server instead of the production server:

```bash
docker run -d -v $(pwd):/app -p 8000:8000 --name api fastapi-app fastapi dev main.py --host 0.0.0.0 --port 8000
```

`fastapi dev` starts the server with auto-reload enabled. `--host 0.0.0.0` makes the server accessible from outside the container. Now open `main.py` on your host machine and change the message from `"Hello, Docker!"` to something else, for example `"Hello, Bind Mounts!"`. Save the file and refresh `http://localhost:8000` in your browser. You will see the updated response without restarting or rebuilding the container. This is the main advantage of bind mounts during development.

### Removing Volumes

```bash
docker volume rm app-data             # Remove a specific volume
docker volume prune                   # Remove all unused volumes
```

`prune` removes only volumes not used by any container. It will prompt for confirmation.

---

## Docker Networking

Docker creates virtual networks that allow containers to communicate with each other and with the outside world.

### Network Types

| Driver   | Description                                                                     |
| -------- | ------------------------------------------------------------------------------- |
| `bridge` | Default. Isolated network on a single host. Containers use names to communicate |
| `host`   | Container shares the host's network directly (no port mapping needed)           |
| `none`   | No networking. Fully isolated container                                         |

### Working with Networks

```bash
docker network create mynet           # Create a custom bridge network
docker network ls                     # List all networks
docker network inspect mynet          # Show network details
docker network rm mynet               # Remove a network
```

### Connecting Containers

```bash
docker network create app-net

docker run -d --name db --network app-net postgres:16
docker run -d --name api --network app-net -p 8000:8000 fastapi-app
```

`--network app-net` attaches the container to the `app-net` network. Containers on the same custom network can reach each other by container name. For example, the `api` container can connect to the database at `db:5432`. The default `bridge` network does not support name-based discovery; you need a custom network for that.

---

## Docker Compose

Docker Compose lets you define and run multi-container applications using a single YAML file. Instead of running multiple `docker run` commands with long flags, you describe everything in `compose.yaml`.

### Example: FastAPI + PostgreSQL

Create `compose.yaml`:

```yaml
services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/mydb
    depends_on:
      - db
    volumes:
      - .:/app

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=mydb
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  db-data:
```

`services` defines the containers to run. `build: .` tells Compose to build an image from the Dockerfile in the current directory. `depends_on` ensures `db` starts before `api`. `environment` sets environment variables inside the container. `volumes` at the top level defines named volumes; `db-data:/var/lib/postgresql/data` persists database files. The `api` service can connect to the database using `db` as the hostname because Compose creates a shared network automatically.

### Compose Commands

```bash
docker compose up                     # Start all services (foreground)
docker compose up -d                  # Start in background (detached)
docker compose up --build             # Rebuild images before starting
docker compose down                   # Stop and remove containers + network
docker compose down -v                # Also remove volumes
docker compose ps                     # List running services
docker compose logs                   # View logs from all services
docker compose logs -f api            # Follow logs for a specific service
docker compose exec api bash          # Open shell in a running service
docker compose stop                   # Stop services without removing
docker compose start                  # Start previously stopped services
```

`up` creates and starts all services defined in the file. `--build` forces a rebuild even if no changes are detected. `down` stops containers, removes them, and removes the network that Compose created. `-v` with `down` also removes named volumes defined in the file. `logs -f` follows log output in real time.

---

## Docker Hub

Docker Hub is the default public registry for Docker images. You can pull public images, push your own, and automate builds.

### Login

```bash
docker login                          # Login with Docker Hub credentials
docker login -u yourusername          # Specify username
```

`docker login` prompts for username and password. Credentials are stored locally.

### Tagging Images

```bash
docker tag fastapi-app yourusername/fastapi-app:1.0
docker tag fastapi-app yourusername/fastapi-app:latest
```

`docker tag` creates a new tag pointing to the same image. The format is `username/repository:tag`. You need to tag images with your Docker Hub username before pushing.

### Pushing Images

```bash
docker push yourusername/fastapi-app:1.0
docker push yourusername/fastapi-app:latest
```

`push` uploads the image layers to Docker Hub. Only layers not already on the registry are uploaded.

### Pulling Images

```bash
docker pull yourusername/fastapi-app:1.0
```

### Complete Workflow

```bash
# 1. Build locally
docker build -t fastapi-app .

# 2. Tag for Docker Hub
docker tag fastapi-app yourusername/fastapi-app:1.0

# 3. Login
docker login

# 4. Push
docker push yourusername/fastapi-app:1.0

# 5. On another machine, pull and run
docker pull yourusername/fastapi-app:1.0
docker run -d -p 8000:8000 yourusername/fastapi-app:1.0
```

### Docker Hub Visibility

- **Public**: Anyone can pull the image.
- **Private**: Only you and collaborators can access it. Docker Hub free accounts allow one private repository.

---

## Multi-Stage Builds

Multi-stage builds let you use multiple `FROM` instructions in a single Dockerfile. Each `FROM` starts a new build stage. You copy only the artifacts you need from one stage to the next, keeping the final image small.

### Example: Build & Production Stages

```dockerfile
# Stage 1: Install dependencies
FROM python:3.14-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Stage 2: Production image
FROM python:3.14-slim

WORKDIR /app
COPY --from=builder /install /usr/local
COPY . .

EXPOSE 8000
CMD ["fastapi", "run", "main.py", "--port", "8000"]
```

`AS builder` names the first stage so it can be referenced later. `--prefix=/install` installs packages to a specific directory instead of the default location. `COPY --from=builder` copies files from the `builder` stage into the current stage. The final image does not include the build tools or caches from the first stage, resulting in a smaller image.

---

## Environment Variables & .env Files

### Setting Environment Variables

```bash
docker run -d -e APP_ENV=production -e DEBUG=false fastapi-app
```

`-e` sets an environment variable inside the container. You can pass multiple `-e` flags.

### Using .env Files

Create `.env`:

```
APP_ENV=production
DEBUG=false
DATABASE_URL=postgresql://user:pass@db:5432/mydb
```

```bash
docker run -d --env-file .env -p 8000:8000 fastapi-app
```

`--env-file .env` loads all variables from the file into the container. Each line should be in `KEY=VALUE` format.

### In Docker Compose

```yaml
services:
  api:
    build: .
    env_file:
      - .env
```

`env_file` loads variables from the specified file into the service container.

---

## Docker Logs & Debugging

### Viewing Logs

```bash
docker logs api                       # Show all logs
docker logs -f api                    # Follow logs in real time
docker logs --tail 50 api             # Show last 50 lines
docker logs --since 1h api            # Show logs from last 1 hour
docker logs -t api                    # Show timestamps
```

`-f` (follow) streams new log entries as they appear. `--tail 50` shows only the last 50 lines instead of the entire log. `--since 1h` filters logs to the last hour; accepts values like `30m`, `2h`, `2025-01-01`. `-t` prefixes each line with a timestamp.

### Container Details

```bash
docker inspect api                    # Full container metadata (JSON)
docker stats                          # Live CPU, memory, network usage
docker top api                        # Show processes running inside container
```

`stats` provides a live, updating view similar to `top` on Linux but for all containers. `top` shows the processes running inside a specific container.

### Debugging a Failed Container

```bash
docker ps -a                          # Find the stopped container
docker logs <container_id>            # Check what went wrong
docker run -it fastapi-app bash       # Start a new container with shell
```

---

## Docker Prune & Cleanup

Docker resources (stopped containers, unused images, dangling layers, volumes) accumulate over time and consume disk space.

```bash
docker container prune                # Remove all stopped containers
docker image prune                    # Remove dangling images (untagged)
docker image prune -a                 # Remove all unused images
docker volume prune                   # Remove all unused volumes
docker network prune                  # Remove all unused networks
docker system prune                   # Remove containers, networks, dangling images
docker system prune -a --volumes      # Remove everything unused (aggressive)
docker system df                      # Show Docker disk usage
```

`prune` removes unused resources and prompts for confirmation. `-a` (all) with `image prune` removes all images not used by a container, not just dangling ones. `system prune` is a combined cleanup that removes stopped containers, unused networks, and dangling images in one command. `-a --volumes` with `system prune` also removes unused images and volumes. `system df` shows how much disk space Docker is using broken down by images, containers, volumes, and build cache.
