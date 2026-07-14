FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY privacy_cover_bot.py db.py ./

CMD ["python", "privacy_cover_bot.py"]
