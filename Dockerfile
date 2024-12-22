#https://fastapi.tiangolo.com/app/docker/#containers-and-processes
# ssh://git@git.belastingdienst.nl:7999/vdambieb/mbk-fastapi.git
#FROM python:3.12-slim
FROM registry.access.redhat.com/ubi9/python-311:latest
#FROM  cir-cn.chp.belastingdienst.nl/belastingdienst/cpet/ubi9/python311:latest

#USER operator
USER root

WORKDIR /app

RUN chmod a+rwx -R /app

RUN mkdir -p /app/Downloads
RUN chmod a+rwx -R /app/Downloads
COPY ./Downloads/modellenbibliotheek.xlsx /app/Downloads/modellenbibliotheek.xlsx
COPY ./Downloads/modellenbibliotheek.xlsx /app/Downloads/modellenbibliotheek_laatste.xlsx
COPY ./Downloads/modellenbibliotheek.xlsx /app/modellenbibliotheek.xlsx



ENV PATH="/root/.local/bin:$PATH"
ENV KEYCLOAK_SERVER_URL="http://localhost:8080/auth"
ENV KEYCLOAK_REALM="testing"
ENV KEYCLOAK_CLIENT_ID="fastapi-keycloak"
ENV KEYCLOAK_CLIENT_SECRET="9866c9bf-3523-4968-a354-9d5839baf76f"

# --- Python Setup ---
COPY requirements.txt /app
RUN pip3 install -r  requirements.txt

# --- Python Setup ---
#RUN pip3 install -i https://nexus.belastingdienst.nl/nexus/repository/pypi-group/simple --trusted-host nexus.belastingdienst.nl  -r requirements.txt

COPY ./src/main.py  /app/main.py

COPY . .

RUN chmod a+rwx -R /app

EXPOSE 8080

CMD ["fastapi", "run", "main.py", "--port", "8080"]
