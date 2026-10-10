FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY recentchange.avsc producer.py ./
# The stream position lives on a volume so a restarted container resumes.
ENV POSITION_FILE=/state/position.json
RUN mkdir /state && chown nobody /state
VOLUME /state
USER nobody
CMD ["python", "producer.py"]
