
# Wholesum network `Prover` CLI

## Overview

Wholesum is a p2p prover network `tailored for ETH L1 block proving`. It builds on top of [SP1](https://docs.succinct.xyz/docs/sp1/introduction) and [Libp2p](https://libp2p.io). The design of the network follows a p2p distributed proving scheme where SP1 jobs are passed around and proved by prover nodes.

### Prerequisites

You would need to get certain environments ready for the prover to function properly.

#### SP1 

To install SP1, please follow the following [guide](https://docs.succinct.xyz/docs/sp1/getting-started/install).

#### Docker

Docker runtime is needed as it is used to run certain `SP1` containers. This awesome [guide](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04) from DigitalOcean is helpful in this regard.

#### MongoDB

Install the MongoDB from [here](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-community-with-docker/). Make sure a Docker container runs and is listenning on `localhost:27017`

#### Valkey

A ValKey container is required to fetch SP1 blobs that are produced during `rsp-subblock` block generation and execution. To run, type `docker run -d --name valkey-server -p 6379:6379 valkey/valkey:8.1`.

### Library dependencies

Download the the following libraries and put them in the parent("..") directory:

- [peyk](https://github.com/WholesumNet/peyk)
- [anbar](https://github.com/WholesumNet/anbar)

### ELFs

You would need Subblock and Aggregate ELFs. Kindly put `agg_elf.bin` and `subblock_elf.bin` in `../elfs` directory. These files are generated during rsp-subblock's block execution process. 
