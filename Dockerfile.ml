FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
COPY requirements-ml.txt ./requirements-ml.txt
RUN pip install --no-cache-dir -r requirements-ml.txt
COPY src ./src
COPY tests ./tests
COPY streamlit_app ./streamlit_app
# Match the official Airflow image's default UID/GID so both containers can
# read/write the same private model directories (mkdtemp uses mode 0700).
USER 50000:0
ENTRYPOINT ["python", "-m", "src.ml"]
CMD ["--help"]
