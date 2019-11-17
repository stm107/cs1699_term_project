FROM gradle:jdk8 as builder

COPY --chown=gradle:gradle . ./
WORKDIR ./
RUN gradle build

CMD ["gradle", "run"]