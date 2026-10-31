FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app
COPY db ./db
COPY frontend ./frontend
COPY tests ./tests
COPY load_simulator.py pytest.ini ./

RUN useradd --create-home --uid 1000 deadbolt && chown -R deadbolt /app
USER deadbolt

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=5 \
  CMD python -c "import os,urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:%s/api/health' % os.environ.get('PORT', '8000'), timeout=2).status == 200 else 1)"

# One worker process on purpose: the micro-batch queue and live metrics are
# in-process state. Scale out by sharding hot products across instances.
# Cloud hosts (Render, Railway, Fly) inject $PORT; locally it defaults to 8000.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --loop uvloop --http httptools --proxy-headers --forwarded-allow-ips='*' --log-level warning"]
