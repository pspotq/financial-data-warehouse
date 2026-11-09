# rrp-gov (rebuild b252, 2026-10-29): the agency data, one writer for dataset gov. Built with the cloud/ folder as the
# context (docker build -f cloud/gov/Dockerfile cloud), so it carries the feeds and market code unchanged and runs both
# with RRP_SUBJECT=gov: CFTC COT, DoD contracts and war.gov from feeds; Treasury (DTS, par yields), EIA and NOAA degree
# days from market.
FROM python:3.12-slim
WORKDIR /app
COPY feeds/requirements.txt ./feeds/requirements.txt
COPY market/requirements.txt ./market/requirements.txt
RUN pip install --no-cache-dir -r feeds/requirements.txt -r market/requirements.txt
COPY feeds/*.py ./feeds/
COPY market/*.py ./market/
COPY gov/run.py ./run.py
ENV RRP_SUBJECT=gov
CMD ["python", "run.py"]
