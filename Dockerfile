FROM ubuntu:15.04
RUN apt-get update && apt-get install -y nano curl

ENV NODE_VERSION 4.4.4
ENV NPM_VERSION 2.15.1

RUN curl -SLO "https://nodejs.org/dist/v$NODE_VERSION/node-v$NODE_VERSION-linux-x64.tar.gz" \
        && tar -xzf "node-v$NODE_VERSION-linux-x64.tar.gz" -C /usr/local --strip-components=1 \
        && rm "node-v$NODE_VERSION-linux-x64.tar.gz" \
        && npm install -g npm@"$NPM_VERSION" \
	&& npm install forever -g \
        && npm cache clear

ENV TERM xterm
WORKDIR /deploy
CMD bash
