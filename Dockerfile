FROM tercen/dartrusttidy:1.0.7

USER root
WORKDIR /operator

RUN git clone https://github.com/tercen/OPERATOR_NAME.git

WORKDIR /operator/OPERATOR_NAME

RUN echo X.X.X && git pull
RUN git checkout X.X.X

RUN R -e "renv::restore(confirm=FALSE)"

ENV TERCEN_SERVICE_URI https://tercen.com

COPY start.R /start.R

ENTRYPOINT [ "R","--no-save","--no-restore","--no-environ","--slave","-f","/start.R"]






