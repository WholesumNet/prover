# Wholesum network `Server` CLI

## Overview

Wholesum network is a p2p verifiable computing network `tailored for ETH L2 sequencer proving`. It builds on top of [Risc0](https://risczero.com/), [Libp2p](https://libp2p.io), and decentralized storage options like [Swarm](https://ethswarm.org) and Filecoin to facilitate verifiable computing at scale. The design of the network follows a p2p parallel proving scheme where Risc0 jobs are passed around, proved, and finally combined into a final proof ready for L1 verification.

## How to run

Bringing a server instance up involves few steps.

### Prerequisites

You would need to get certain environments ready for the server to function properly.

#### 1- Docker

Docker runtime is needed as it is used to run `Risc0` containers. This awesome [guide](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04) from DigitalOcean is helpful in this regard.

### 2- Lighthouse@Filecoin/Swarmy@Swarm

- Lighthouse:  
  You would need a subscription plan from [Lighthouse](https://docs.lighthouse.storage/lighthouse-1/quick-start) to run the server. Please obtain an api key and specify it with `-d` flag when running the server.
  
- Swarmy:
  Still under develepment.
  

### 3- Dependencies

To run a server agent, you would first need to fork the following libraries and put them in the parent("..") directory of the server:

- [comms](https://github.com/WholesumNet/comms)
- [dStorage](https://github.com/WholesumNet/dStorage)
- [jocker](https://github.com/WholesumNet/jocker)

### USAGE

<pre>
Wholesum is a P2P verifiable computing marketplace and this program is a CLI for server nodes.
Usage: server [OPTIONS]
Options:
  -d, --dstorage-key-file <DSTORAGE_KEY_FILE>  
      --dev                                    
  -k, --key-file <KEY_FILE>                    
  -h, --help                                   Print help
  -V, --version                                Print version
</pre>
