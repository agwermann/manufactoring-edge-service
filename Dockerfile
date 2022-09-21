FROM python:3.9-slim-bullseye

# Setup venv
COPY . /app
WORKDIR /app

# Config virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install Dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Setup and start app
EXPOSE 8080
ENTRYPOINT [ "python", "main.py" ]