FROM mcr.microsoft.com/dotnet/framework/runtime:4.8-windowsservercore-ltsc2022
SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop';"]

# Instalar Python
RUN Invoke-WebRequest -Uri https://www.python.org/ftp/python/3.11.0/python-3.11.0-amd64.exe -OutFile python.exe; \
    Start-Process python.exe -Wait -ArgumentList '/quiet InstallAllUsers=1 PrependPath=1'; \
    Remove-Item python.exe

# Instalar ADOMD.NET
RUN Invoke-WebRequest -Uri https://download.microsoft.com/download/8/7/2/872BCECA-C849-4B40-8EBE-21D48CDF1456/EN/x64/SQL_AS_ADOMD.msi -OutFile adomd.msi; \
    Start-Process msiexec.exe -Wait -ArgumentList '/i adomd.msi /quiet /norestart'; \
    Remove-Item adomd.msi

WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --upgrade pip; pip install -r requirements.txt

COPY . .
EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]