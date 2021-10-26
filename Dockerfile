FROM tercen/pamsoft_grid:1.0.5


ENV RENV_VERSION 0.9.2


RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cran.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"

COPY . /operator

WORKDIR /operator

RUN R  --vanilla -e "renv::restore(confirm=FALSE)"

ENV TERCEN_SERVICE_URI https://tercen.com
ENV LD_LIBRARY_PATH .:/opt/mcr/v99/runtime/glnxa64:/opt/mcr/v99/bin/glnxa64:/opt/mcr/v99/sys/os/glnxa64:/opt/mcr/v99/sys/opengl/lib/glnxa64

ENTRYPOINT [ "R","--no-save","--no-restore","--no-environ","--slave","-f","main.R", "--args"]
CMD [ "--taskId", "someid", "--serviceUri", "https://tercen.com", "--token", "sometoken"]