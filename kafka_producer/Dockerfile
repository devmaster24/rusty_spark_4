FROM rust:slim-buster

WORKDIR /usr/src/kafka_producer

# Install build tools
RUN apt-get update \
    && apt-get install -y build-essential openssl libssl-dev zlib1g-dev \
    && apt-get clean

COPY ./src ./src
COPY ./Cargo* ./

RUN cargo install --path .

CMD [ "kafka_producer" ]

